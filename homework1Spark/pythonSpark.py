

TASK 2
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
records = sc.textFile("/home/iori/Downloads/FineFoodReviews/amazon/1999_2006.csv")
rows = records.map(lambda line: line.split("\t"))
userToFavouriteProducts=rows.map(lambda x: [x[1],x[2],x[6]]).map(lambda n: (str(n[1]),str(str(n[2])+"-"+str(n[0])))).groupByKey().map(lambda x: {x[0]: sorted(list(x[1]))[-10:]}).collect()
for row in sorted(userToFavouriteProducts):
    print row

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


TASK 1
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
records = sc.textFile("/home/mattia/Scaricati/amazon/1999_2006.csv")
seqOp = lambda x, y : (float(x[0]) + float(y), float(x[1]) + 1)
combOp = lambda x, y : (float(x[0]) + float(y[0]), float(x[1]) + float(b[1]))
rows = records.map(lambda line: line.split("\t"))
rows.map(lambda x: [str(datetime.datetime.fromtimestamp(int(x[7])).strftime('%Y-%m')),x[1],x[6]]).map(lambda n: ((str(n[0]) + " " + str(n[1])), str(n[2]))).aggregateByKey((0,0), seqOp, combOp).mapValues(lambda v: v[0]/v[1]).sortByKey().map(lambda (k,v): (k.split(" ")[0], str(v) + " " + k.split(" ")[1])).groupByKey().sortByKey().map(lambda x: {x[0]: sorted(list(x[1]))[-5:]}).collect()

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
