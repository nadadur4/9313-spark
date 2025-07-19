from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

class Project2:
    def run(self, inputPath, outputPath, stopwords, topic, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        nStopwords = int(stopwords)
        nTopics = int(topic)
        nTopPerYear = int(k)

        # We parse the text using various methods from pyspark.sql.functions
        # and arrange our new columns into an initial DataFrame
        rawText = spark.read.text(inputPath)
        parts = split(rawText.value, ',', 2)
        yearCol = substring(parts[0], 1, 4)
        headlineCol = array_distinct(split(lower(parts[1]), " "))

        headlinesDF = (rawText
                       .withColumn("year", yearCol)
                       .withColumn("headline", headlineCol)
                       .drop("value")
        )

        # alias() is like SQL as
        rawWordFreqs = (headlinesDF
                        .select(explode(col("headline")).alias("word"))
                        .groupBy("word")
                        .count()
        )

        stopwordsDF = (rawWordFreqs
                       .orderBy(["count", "word"], ascending=[False, True])
                       .limit(nStopwords)
                       .select(col("word").alias("stopword"))
        )

        hlExploded = (headlinesDF
                      .select("year", explode(col("headline")).alias("word"))
        )

        # Left join every word in every headline with the list of stopwords.
        # All non-stopword rows will have NULL in the stopword field
        # New structure: [year | word] for every word in every headline
        hleFiltered = (hlExploded
                       .join(broadcast(stopwordsDF),
                             hlExploded.word == stopwordsDF.stopword,
                             "left"
                       )
                       .filter(col("stopword").isNull())
                       .select("year", "word")
        )
                         
        docFreqs = (hleFiltered
                    .groupBy("word")
                    .count()
        )

        topicTerms = (docFreqs
                      .orderBy(["count", "word"], ascending=[False, True])
                      .limit(nTopics)
                      .select(col("word").alias("topicTerm"))
        )

        # Now we just do an inner join (since we only want to keep topic terms)
        hleTopicsOnly = (hleFiltered
                         .join(broadcast(topicTerms),
                               hleFiltered.word == topicTerms.topicTerm
                         )
                         .select("year", "word")
        )

        # Grouping by year and topic to count occurrences within year,
        # then we group by just year to count total no. topic terms
        yearTopicPairCounts = (hleTopicsOnly
                               .groupBy("year", "word")
                               .count()
                               .select("year", "word",
                                       col("count").alias("wordCount")
                               )
        )
        
        yearTopicTotal = (yearTopicPairCounts
                          .groupBy("year")
                          .count()
                          .select("year",
                                  col("count").alias("yearTotal")
                          )
        )

        # We use Window allows for partitioning by year (across rows)
        yearWindow = (Window
                .partitionBy("year")
                .orderBy(desc("wordCount"), "word")
        )

        # WE concatenate word and its frequency (wordCount) to make wordC
        # Then, we take k-top topic terms using our window to rank terms by count
        # withColumn expects columns (according to documentation), so lit()
        # just converts the string to a "column"
        topKPerYear = (yearTopicPairCounts
                       .join(yearTopicTotal, "year")
                       .withColumn("wordC",
                                   concat(col("word"), lit("\t"), col("wordCount"))
                       )
                       .withColumn("yearTopicCounter", row_number().over(yearWindow))
                       .filter(col("yearTopicCounter") <= nTopPerYear)      
        )

        # We make a struct of yearTopicCounter and the line, so that the sort
        # order is retained. Note: transform function is sort of like how we use
        # map or mapValues in RDD implementation
        topKPerYear = (topKPerYear
                       .groupBy("year", "yearTotal")
                       .agg(sort_array(
                            collect_list(struct("yearTopicCounter", "wordC"))
                            ).alias("wordList")
                       )
                       .withColumn("wordCountArrs", 
                                   transform(col("wordList"), 
                                             lambda arr: arr["wordC"]
                                   )
                       )
        )

        # Finally, we use concat_ws (like in SQL) to format output as required
        finalDF = (topKPerYear
                   .select(concat_ws(":", col("year"), col("yearTotal"))
                           .alias("year:total"),
                           concat_ws("\n", col("wordCountArrs"))
                           .alias("wordCounts")
                   )
                   .select(concat(col("year:total"), lit("\n"), col("wordCounts"))
                           .alias("entry")
                   )
                   .orderBy("year:total")
                   
        )

        finalDF.coalesce(1).write.text(outputPath)
        
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

