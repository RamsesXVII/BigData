TASK1
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

records = sc.textFile(sys.argv[1])
seqOp = lambda x, y : (float(x[0]) + float(y), float(x[1]) + 1)
combOp = lambda x, y : (float(x[0]) + float(y[0]), float(x[1]) + float(y[1]))

rows = records.map(lambda line: line.split("\t"))
result = rows.map(lambda x: [str(datetime.datetime.fromtimestamp(int(x[7])).strftime('%Y-%m')),x[1],x[6]])
             .map(lambda n: ((str(n[0]) + " " + str(n[1])), str(n[2]))).aggregateByKey((0,0), seqOp, combOp)
             .mapValues(lambda v: v[0]/v[1])
             .sortByKey()
             .map(lambda (k,v): (k.split(" ")[0], str(v) + " " + k.split(" ")[1]))
             .groupByKey()
             .sortByKey()
             .map(lambda x: {x[0]: sorted(list(x[1]))[-5:]})
             .collect()

out_file= open(sys.argv[2], "w+")

for row in sorted(result):
    out_file.write(str(row))
    out_file.write("\n")

out_file.close()

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

TASK2
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

records = sc.textFile(sys.argv[1])
rows = records.map(lambda line: line.split("\t"))

userToFavouriteProducts=rows.map(lambda x: [x[1],x[2],x[6]])
                            .map(lambda n: (str(n[1]),str(str(n[2])+"-"+str(n[0]))))
                            .groupByKey()
                            .map(lambda x: {x[0]: sorted(list(x[1]))[-10:]})
                            .collect()

out_file= open(sys.argv[2], "w+")

for row in sorted(userToFavouriteProducts):
    out_file.write(str(row))
    out_file.write("\n")

out_file.close()
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

TASK3
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

records = sc.textFile(sys.argv[1])
rec= sc.parallelize(records.collect())

rows = rec.map(lambda line: line.split("\t"))
		  .filter(lambda x: int(x[6])>=4)

a=rows.map(lambda x: (x[1],[x[2], x[6]]))

a=a.join(a)
	.filter(lambda (x,(a,b)): a[0]<b[0])
	.map(lambda (x,(b,a)): ((a[0],b[0]),x))
	.groupByKey()
	.filter(lambda (x,y): len(set(y))>2)
	.sortBy(lambda a: a[0])
	.saveAsTextFile(sys.argv[2])