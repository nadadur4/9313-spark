from pyspark import SparkContext, SparkConf
from operator import add
import sys

class Answer:
    def run(self, inputPath, outputPath):
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

        with open("/home/nadserver/spark-home/labs/05/Q3.txt", "w") as Q3:
            print(distinctItemTypes, file=Q3)

        # Question 4
        bidsPerItemType = (
            auctionRDD.map(lambda line: (line[itemtype], 1))
                      .reduceByKey(add)
        )

        with open("/home/nadserver/spark-home/labs/05/Q4.txt", "w") as Q4:
            print(bidsPerItemType.collect(), file=Q4)

        # Question 5
        maximumBidAuction = (
            auctionRDD.map(lambda line: (line[aucid], 1))
                      .reduceByKey(add)
                      .reduce(max)
        )

        with open("/home/nadserver/spark-home/labs/05/Q5.txt", "w") as Q5:
            print(maximumBidAuction, file=Q5)

        # Question 6
        top5Items = (
            auctionRDD.map(lambda line: (line[aucid], 1))
                      .reduceByKey(add)
                      .sortBy(lambda line: line[1], ascending=False)
                      .take(5)
        )

        with open("/home/nadserver/spark-home/labs/05/Q6.txt", "w") as Q6:
            print(top5Items, file=Q6)

        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Wrong inputs")
        sys.exit(-1)
    Answer().run(sys.argv[1], sys.argv[2])
