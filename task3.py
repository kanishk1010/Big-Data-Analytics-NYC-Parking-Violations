import sys
from decimal import Decimal
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task3")
sc = SparkContext(conf=conf)
line1 = sc.textFile(sys.argv[1], 1)
line1 = line1.mapPartitions(lambda x: reader(x))
amount = line1.map(lambda x: (x[2],Decimal(x[12])))
sum = amount.reduceByKey(lambda x, y: x + y)
licensetype = line1.map(lambda x: (x[2], 1))
licensecount =  licensetype.reduceByKey(lambda x, y: x + y)
sumandavg=sum.join(licensecount)
sumandavg=sumandavg.map(lambda x: (x[0], x[1][0], Decimal((x[1][0]/x[1][1]).quantize(Decimal('.01')))))
sumandavg.map(lambda (k, v, l): "{0}\t{1}, {2}".format(k, v, l)).saveAsTextFile("task3.out")