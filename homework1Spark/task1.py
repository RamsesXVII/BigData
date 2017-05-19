from pyspark.sql import SparkSession
import datetime
import time
import sys

spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

start = int(round(time.time() * 1000))

records = sc.textFile(sys.argv[1])
seqOp = lambda x, y : (float(x[0]) + float(y), float(x[1]) + 1)
combOp = lambda x, y : (float(x[0]) + float(y[0]), float(x[1]) + float(y[1]))
rows = records.map(lambda line: line.split("\t"))
result = rowsmap(lambda x: ((str(datetime.datetime.fromtimestamp(int(x[7])).strftime('%Y-%m')) + " " +x[1]),x[6])).aggregateByKey((0,0), seqOp, combOp).mapValues(lambda v: v[0]/v[1]).map(lambda (k,v): (k.split(" ")[0], str(v) + " " + k.split(" ")[1])).groupByKey().sortByKey().map(lambda x: {x[0]: sorted(list(x[1]))[-5:]}).collect()

out_file= open(sys.argv[2], "w+")

for row in sorted(result):
    out_file.write(str(row))
    out_file.write("\n")

out_file.close()

end =  int(round(time.time() * 1000))
print (end - start)

