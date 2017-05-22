from pyspark.sql import SparkSession
import time
import sys

spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

start = int(round(time.time() * 1000))

records = sc.textFile(sys.argv[1])
rows = records.map(lambda line: line.split("\t"))
userToFavouriteProducts=rows.map(lambda x: (x[2],x[6] +"-"+ x[1])).groupByKey().map(lambda x: {x[0]: sorted(list(x[1]))[-10:]}).collect()

end = int(round(time.time() * 1000))

out_file= open(sys.argv[2], "w+")

for row in sorted(userToFavouriteProducts):
    out_file.write(str(row))
    out_file.write("\n")

out_file.close()
print (end - start)
