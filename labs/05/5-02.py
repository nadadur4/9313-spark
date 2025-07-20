from pyspark import SparkContext, SparkConf
import sys

class Answer:
    def run(self, inputPath, outputPath)
        conf = SparkConf().setAppName("problem2")
        sc = SparkContext(conf=conf)
        
        auctionRDD = (sc
                      .textFile(inputPath)
                      .map(lambda line: line.split(","))
        )
        
        (aucid, bid, bidtime, bidder,
         bidderrate, openbid, price,
         itemtype, dtl) = range(9)

        # Question 3
        distinctItemTypes = (
            auctionRDD.map(lambda line: line[itemtype])
                      .distinct()
                      .count()
        )

        print(distinctItemTypes.collect())

        # Question 4
        bidsPerItemType = (
            auctionRDD.map(lambda line: (line[itemtype], 1))
                      .reduceByKey()
        )

        print(bidsPerItemType.collect())

        # Question 5
        maximumBidAuction = (
            auctionRDD.map(lambda line: (line[aucid], 1))
                      .reduceByKey()
                      .reduce(lambda a, b: max(a[1], b[1]))
        )

        print(maximumBidAuction.collect())

        # Question 6
        top5Items = (
            auctionRDD.map(lambda line: (line, 1))
                      .reduceByKey()
                      .sortBy(lambda line: line[1])
                      .take(5)
        )

        print(top5Items.collect())

        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Wrong inputs")
        sys.exit(-1)
    Answer().run(sys.argv[1], sys.argv[2])
