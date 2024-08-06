from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Total Marks Archived By Female and Male')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../files/StudentData.csv')

rdd2 = rdd.map(lambda x: (x.split(',')[1], x.split(',')[5]))

rdd3 = rdd2.reduceByKey(lambda x,y: int(x)+int(y))

print(rdd3.collect())