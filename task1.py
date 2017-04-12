import sys
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task1")
sc = SparkContext(conf=conf)
line1 = sc.textFile(sys.argv[1], 1)
line1 = line1.mapPartitions(lambda x: reader(x))
line2 = sc.textFile(sys.argv[2], 1)
line2 = line2.mapPartitions(lambda x: reader(x))
allviolation = line1.map(lambda x: (x[0],(x[14],x[6],x[2],x[1])))
openviolation = line2.map(lambda x: (x[0],1))
result = allviolation.subtractByKey(openviolation)
result = result.sortByKey().map(lambda x: (x[0],x[1][0],x[1][1],x[1][2], x[1][3]))
result.map(lambda (a,b,c,d,e): "{0}\t{1}, {2}, {3}, {4}".format(a,b,c,d,e)).saveAsTextFile("task1.out")