import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from operator import add

PREFIX, NUMID, RECID, X, Y, TERMS = 0, 1, 2, 3, 4, 5

def lineParse(line, prefix=''):
    recId, coOrd, terms = line.strip().split('#')
    numId = int(recId[1:])
    x, y = coOrd.strip('()').split(',')
    x, y = float(x), float(y)
    terms = set(terms.split())
    parsedLine = (prefix, numId, recId, x, y, terms)
    return parsedLine

def 

class project3:
    def run(self, inputpathA, inputpathB, outputpath, d, s):
        conf = SparkConf().setAppName("Project3")
        sc = SparkContext(conf=conf)
        
        rddA = sc.textFile(inputpathA).map(lambda line: lineParse(line, 'A'))
        rddB = sc.textFile(inputpathB).map(lambda line: lineParse(line, 'B'))

        termPairsA = rddA.flatMap(lambda rec: [(term, 1) for term in rec[TERMS]])
        termPairsB = rddB.flatMap(lambda rec: [(term, 1) for term in rec[TERMS]])
        termFreqs = termPairsA.union(termPairsB).reduceByKey(add)
        termFreqsList = termFreqs.collect()
        
        termOrder = (termFreqs
                      .sortBy(lambda pair: (pair[1], pair[0]))
                      .map(lambda pair: pair[0])
                      .zipWithIndex()
                      .collect()
        )
        bcTermOrder = sc.broadcast(set(termOrder))


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
