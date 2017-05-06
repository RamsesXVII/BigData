Not workging, but useful to understand the process

TASK1
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

records = sc.textFile("amazon/1999_2006.csv")
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

for row in sorted(result):
	print row
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

TASK2
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

records = sc.textFile("/home/iori/Downloads/FineFoodReviews/amazon/1999_2006.csv")
rows = records.map(lambda line: line.split("\t"))

userToFavouriteProducts=rows.map(lambda x: [x[1],x[2],x[6]])
                            .map(lambda n: (str(n[1]),str(str(n[2])+"-"+str(n[0]))))
                            .groupByKey()
                            .map(lambda x: {x[0]: sorted(list(x[1]))[-10:]})
                            .collect()

for row in sorted(userToFavouriteProducts):
    print row
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

TASK3
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

records = sc.textFile("/home/iori/Downloads/FineFoodReviews/amazon/1999_2006.csv")
rows = records.map(lambda line: line.split("\t"))

a=rows.map(lambda x: [x[1],x[2],x[6]])
      .filter(lambda x: int(x[2])>=4)
      .map(lambda n: (str(n[0]),[str(n[1]),str(n[2])]))


a=a.join(a)
   .filter(lambda (x,(a,b)): a[0]<b[0])
   .map(lambda (x,(b,a)): ((a[0],b[0]),x))
   .groupByKey()
   .filter(lambda (x,y): len(set(y))>2)
   .sortByKey()
   .collect()


for c in a:
	sys.stdout.write(str(c[0])+" ")
	for d in list(c[1]):
		sys.stdout.write(d+";")
	sys.stdout.write("\n")
