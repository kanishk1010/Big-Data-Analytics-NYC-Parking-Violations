import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task4")
sc = SparkContext(conf=conf)
line1 = sc.textFile(sys.argv[1], 1)
line1 = line1.mapPartitions(lambda x: reader(x))
state = line1.map(lambda x: (("NY" if x[16]=="NY" else "Other" ),1)).reduceByKey(lambda x, y: x + y)
state.map(lambda (k, v): "{0}\t{1}".format(k, v)).saveAsTextFile("task4.out")
