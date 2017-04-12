import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task2")
sc = SparkContext(conf=conf)
line1 = sc.textFile(sys.argv[1], 1)
line1 = line1.mapPartitions(lambda x: reader(x))
violationcodes = line1.map(lambda x: (x[2],1)).reduceByKey(lambda x, y: x + y)
violationcodes.map(lambda (k, v): "{0}\t{1}".format(k, v)).saveAsTextFile("task2.out")