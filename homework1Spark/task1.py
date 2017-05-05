from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

import datetime
records = sc.textFile("amazon/1999_2006.csv")
seqOp = lambda x, y : (float(x[0]) + float(y), float(x[1]) + 1)
combOp = lambda x, y : (float(x[0]) + float(y[0]), float(x[1]) + float(y[1]))
rows = records.map(lambda line: line.split("\t"))
result = rows.map(lambda x: [str(datetime.datetime.fromtimestamp(int(x[7])).strftime('%Y-%m')),x[1],x[6]]).map(lambda n: ((str(n[0]) + " " + str(n[1])), str(n[2]))).aggregateByKey((0,0), seqOp, combOp).mapValues(lambda v: v[0]/v[1]).sortByKey().map(lambda (k,v): (k.split(" ")[0], str(v) + " " + k.split(" ")[1])).groupByKey().sortByKey().map(lambda x: {x[0]: sorted(list(x[1]))[-5:]}).collect()
for row in sorted(result):
	print row