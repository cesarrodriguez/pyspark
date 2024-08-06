from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Reduce By Key')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_distinct.txt')

rdd2 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))


rdd3 = rdd2.reduceByKey(lambda x,y: x+y)

print(rdd3.collect())