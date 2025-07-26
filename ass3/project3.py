import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class project3:
    def run(self, inputpathA, inputpathB, outputpath, d, s):
        # You can use either RDD or DataFrame APIs

        
if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
