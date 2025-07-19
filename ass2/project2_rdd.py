from pyspark import SparkContext, SparkConf
from operator import add
import sys
import math

class Project2:           
    def run(self, inputPath, outputPath, stopwords, topic, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)

        nStopwords = int(stopwords)
        nTopics = int(topic)
        nTopPerYear = int(k)

        textRDD = sc.textFile(inputPath)

        verifiedLines = (textRDD
                         .map(lambda line: line.split(",", 1))
                         .filter(lambda line: len(line) == 2)
        )

        # Converts read-in lines to a tuple of (year, headline). Headline is
        # converted to set type to only show each word once
        headlinesSet = (verifiedLines
                        .map(lambda line: (line[0][:4], set(line[1].lower().split())))
        )

        # We then flatten all headline sets to get a list of words, and
        # count these to get raw term frequency
        headlineWordList = headlinesSet.flatMap(lambda x: x[1])
        rawWordFreqs = (headlineWordList
                        .map(lambda word: (word, 1))
                        .reduceByKey(add)
        )

        # Since words (keys) must be sorted alphabetically (ascending sort)
        # and the numerical values must be sorted in descending order
        # we use (-pair[1]) to do the descending sort
        stopwordsList = (rawWordFreqs
                         .sortBy(lambda pair: (-pair[1], pair[0]))
                         .map(lambda pair: pair[0])
                         .take(nStopwords)
        )
        
        # Broadcast stopwords to retain in memory within nodes
        # as in Lecture 4.2
        stopwordsBC = sc.broadcast(set(stopwordsList))

        # mapValues (Lec 4.2) maps func. on pair values only
        filteredHeadlines = (headlinesSet
                             .mapValues(
                                lambda words: set(
                                    word for word in words
                                    if word not in stopwordsBC.value
                                )
                             )
        )

        docFreqs = (filteredHeadlines
                    .flatMap(lambda pair: pair[1])
                    .map(lambda word: (word, 1))
                    .reduceByKey(add)
        )

        topicTerms = (docFreqs
                      .sortBy(lambda pair: (-pair[1], pair[0]))
                      .map(lambda pair: pair[0])
                      .take(nTopics)
        )

        topicTermsBC = sc.broadcast(set(topicTerms))

        topicsOnly = (filteredHeadlines
                      .mapValues(
                        lambda words: set(
                            word for word in words
                            if word in topicTermsBC.value
                        )
                      )
        )

        yearTopicPairCounts = (topicsOnly
                               .flatMap(lambda pair: [((pair[0], word), 1)
                                                     for word in pair[1]]
                               )
                               .reduceByKey(add)
        )

        # note: 'ytc' is ((year, topic), count) as in the tuples from above
        # The keys/values are rearranged from ((y, t), c) to (y, (t, c))
        topPerYear = (yearTopicPairCounts
                      .map(lambda ytc: (ytc[0][0], (ytc[0][1], ytc[1])))
        )

        # groupByKey (Lec 4.2) groups all values with same key under one
        # key-value pair as a sequence of values: RDD[(K, V)] => RDD[(K, Seq[V])]
        pairsGroupedByYear = topPerYear.groupByKey()
        
        # FInally we add the total number of topic words for the year to the key
        # e.g. ((year, total), [(topic, count), (topic, count)... ]) before sorting
        # the topic terms, and picking the top k
        topKPerYear = (pairsGroupedByYear
                       .mapValues(list)
                       .map(lambda kv: (
                           (kv[0], len(kv[1])),
                           sorted(kv[1], key=lambda tc: (-tc[1], tc[0]))
                           )
                       )
                       .mapValues(lambda seq: seq[:nTopPerYear])
        )

        # sorting by year (asc.) and format output as required
        finalRDD = (topKPerYear
                    .sortByKey()
                    .map(lambda kv:
                         f"{kv[0][0]}:{kv[0][1]}\n" +
                         "\n".join(f"{t}\t{c}" for t, c in kv[1])
                    )
        )

        finalRDD.coalesce(1).saveAsTextFile(outputPath)

        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

