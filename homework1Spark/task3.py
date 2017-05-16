from pyspark.sql import SparkSession
import sys
import time

spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

start = int(round(time.time() * 1000))

records = sc.textFile(sys.argv[1])
rec= sc.parallelize(records.collect())

rows = rec.map(lambda line: line.split("\t"))
a=rows.map(lambda x: (x[1],[x[2],x[6]])).filter(lambda (k,v): int(v[1])>=4)

a=a.join(a).filter(lambda (x,(a,b)): a[0]<b[0]).map(lambda (x,(b,a)): ((a[0],b[0]),x)).groupByKey().filter(lambda (x,y): len(set(y))>2).sortBy(lambda a: a[0]).collect()

out_file= open(sys.argv[2], "w+")

i=0

for c in a:
	i+=1
	out_file.write(str(c[0])+" ")
	for d in list(c[1]):
		out_file.write(d+";")
	out_fle.write("\n")

out_file.close()

end = int(round(time.time() * 1000))
print (end - start)
