import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from operator import add

def lineParse(line, prefix=''):
    recId, coOrd, terms = line.strip().split('#')
    numId = int(recId[1:])
    x, y = coOrd[1:-1].split(',')
    x, y = float(x), float(y)
    tokens = set(terms.split())

    parsedLine = (prefix, numId, recId, x, y, tokens)
    return parsedLine


class project3:
    def run(self, inputpathA, inputpathB, outputpath, d, s):
        conf = SparkConf().setAppName("Project3")
        sc = SparkContext(conf=conf)
        
        rddA = sc.textFile(inputpathA).map(lambda line: lineParse(line, 'A'))
        rddB = sc.textFile(inputpathB).map(lambda line: lineParse(line, 'B'))


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
