from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
records = sc.textFile("/home/iori/Downloads/FineFoodReviews/amazon/1999_2006.csv")
rows = records.map(lambda line: line.split("\t"))
a=rows.map(lambda x: [x[1],x[2],x[6]]).filter(lambda x: int(x[2])>=4).map(lambda n: (str(n[0]),[str(n[1]),str(n[2])]))

a=a.join(a).filter(lambda (x,(a,b)): a[0]<b[0]).map(lambda (x,(b,a)): ((a[0],b[0]),x)).groupByKey().filter(lambda (x,y): len(set(y))>2).sortByKey().collect()

i=0

for c in a:
	i+=1
	sys.stdout.write(str(c[0])+" ")
	for d in list(c[1]):
		sys.stdout.write(d+";")
	sys.stdout.write("\n")


sys.stdout.write(str(i))

