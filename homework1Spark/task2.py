from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext

records = sc.textFile("amazon/dati.csv")
rows = records.map(lambda line: line.split("\t"))
userToFavouriteProducts=rows.map(lambda x: [x[1],x[2],x[6]]).map(lambda n: (str(n[1]),str(str(n[2])+"-"+str(n[0])))).groupByKey().map(lambda x: {x[0]: sorted(list(x[1]))[-10:]}).collect()
for row in sorted(userToFavouriteProducts):
    print row


