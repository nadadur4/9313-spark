import sys
import math
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from operator import add

DATASET, NUMID, X, Y, TERMS, PREFIX, PLEN = 0, 1, 2, 3, 4, 5, 6


def lineParse(line):
    recId, coOrd, terms = line.strip().split('#')
    dataset = recId[0]
    numId = int(recId[1:])
    x, y = coOrd.strip('()').split(',')
    x, y = float(x), float(y)
    terms = set(terms.split())
    parsedLine = (dataset, numId, x, y, terms)
    return parsedLine


def recAddPrefix(rec, bcTermOrder, s):
    # For prefix filtering, to avoid all-pair comparison
    # calculated as per Lecture 7.1 slide 28
    dataset, numId, x, y, terms = rec
    termOrder = bcTermOrder.value
    orderedTerms = sorted(terms, key=lambda term: (termOrder[term], term))
    n = len(orderedTerms)
    l = int(math.ceil(n * s))
    prefixLen = n - l + 1
    prefix = tuple(orderedTerms[:prefixLen])
    prefixRec = (dataset, numId, x, y, terms, prefix, prefixLen)
    return prefixRec


def makeGridWithShorter(rec, d):
    # Yield a grid with cells of distance d (generator fn.)
    # emitting neighbouring cells of the shorter dataset
    # (we use the shorter set for less shuffling)
    dataset, numId, x, y, terms, prefix, prefixLen = rec
    cellX, cellY = int(math.floor(x / d)), int(math.floor(y / d))
    for xOffset in (-1, 0, 1):
        for yOffset in (-1, 0, 1):
            for prefTerm in prefix:
                yield ((cellX + xOffset, cellY + yOffset, prefTerm),
                       (dataset, numId, x, y, terms))


def makeGridWithLonger(rec, d):
    # Yield a grid with cells of distance d (generator fn.)
    # for the longer dataset (to be used to check if within the "neighbourhood"
    # of the shorter set)
    dataset, numId, x, y, terms, prefix, prefixLen = rec
    cellX, cellY = int(math.floor(x / d)), int(math.floor(y / d))
    for prefTerm in prefix:
        yield ((cellX, cellY, prefTerm), (dataset, numId, x, y, terms))


def euclideanDistance(xA, yA, xB, yB):
    return math.sqrt((xA - xB) ** 2 + (yA - yB) ** 2)


def jaccardSimilarity(termsA, termsB):
    if len(termsA) == 0 and len(termsB) == 0:
        return 1.0
    intersectionLen = len(termsA.intersection(termsB))
    if intersectionLen == 0:
        return 0.0
    unionLen = len(termsA.union(termsB))
    return float(intersectionLen) / float(unionLen)


def qualifyPair(pair, d, s):
    recA, recB = pair
    datasetA, numIdA, xA, yA, termsA = recA
    datasetB, numIdB, xB, yB, termsB = recB
    pairDist = euclideanDistance(xA, yA, xB, yB)
    if pairDist > d:
        return None
    pairSim = jaccardSimilarity(termsA, termsB)
    if pairSim < s:
        return None
    return (numIdA, numIdB, pairDist, pairSim)


class project3:
    def run(self, inputpathA, inputpathB, outputpath, d, s):
        d = float(d)
        s = float(s)

        conf = SparkConf().setAppName("Project3")
        sc = SparkContext(conf=conf)

        rddA = sc.textFile(inputpathA).map(lineParse)
        rddB = sc.textFile(inputpathB).map(lineParse)

        # We get the document frequency of terms across A and B sets
        termPairsA = rddA.flatMap(lambda rec: [(term, 1) for term in rec[TERMS]])
        termPairsB = rddB.flatMap(lambda rec: [(term, 1) for term in rec[TERMS]])
        docFreqs = termPairsA.union(termPairsB).reduceByKey(add)

        # We zip with index, then collect as map (which is a dict)
        # We use this dict to be able to index this term order
        # to be able to sort terms within each record with this order
        termOrder = (docFreqs
                     .sortBy(lambda pair: (pair[1], pair[0]))
                     .map(lambda pair: pair[0])
                     .zipWithIndex()
                     .collectAsMap()
        )
        # Broadcast to save in memory as per lecture 4.2
        bcTermOrder = sc.broadcast(termOrder)

        rddAPrefix = rddA.map(lambda rec: recAddPrefix(rec, bcTermOrder, s))
        rddBPrefix = rddB.map(lambda rec: recAddPrefix(rec, bcTermOrder, s))

        # We get the total number of prefix words across each set
        # so only the shorter set is usedto build the neighbour cell grid
        prefixSumA = rddAPrefix.map(lambda rec: rec[PLEN]).sum()
        prefixSumB = rddBPrefix.map(lambda rec: rec[PLEN]).sum()

        # As per the makegrid func. defn. 8 neighbours are generated for
        # the shorter set
        if prefixSumA < prefixSumB:
            shorterRDD, longerRDD = rddAPrefix, rddBPrefix
            shorter, longer = 'A', 'B'
        else:
            shorterRDD, longerRDD = rddBPrefix, rddAPrefix
            shorter, longer = 'B', 'A'

        gridShorter = (shorterRDD
                       .flatMap(lambda rec: makeGridWithShorter(rec, d))
        )
        gridLonger = (longerRDD
                      .flatMap(lambda rec: makeGridWithLonger(rec, d))
        )

        # We now apply the filtering by using join
        filtered = (gridShorter
                    .join(gridLonger)
                    .map(lambda pair: (pair[1][0], pair[1][1]))
        )

        # Apply the criteria as required (similarity and distancee)
        qualified = (filtered
                     .map(lambda pair: qualifyPair(pair, d, s))
                     .filter(lambda rec: rec is not None)
        )

        # and finally, get distinct qualified pairs, by using A-id and B-id
        # as key before reducing
        qualifiedDistinct = (qualified
                             .map(lambda rec: ((rec[0], rec[1]), rec))
                             .reduceByKey(lambda rec1, rec2: rec1)
                             .map(lambda rec: rec[1])
        )

        # If order was reversed (B, A instead of A, B) earlier
        # for the purpose of making the grid, switch it back
        # then We sort by A-id, then B-id
        if shorter == 'B':
            qualifiedDistinct = (qualifiedDistinct
                                 .map(lambda rec:
                                      (rec[1], rec[0], rec[2], rec[3])
                                 )
                                 .sortBy(lambda rec: (rec[0], rec[1]))
            )

        # as per example, output seems to force numbers to 6 decimal places
        output = (qualifiedDistinct
                  .map(lambda rec: f"(A{rec[0]},B{rec[1]}):"
                                   f"{rec[2]:.6f},{rec[3]:.6f}"
                  )
        )

        output.coalesce(1).saveAsTextFile(outputpath)
        sc.stop()


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

