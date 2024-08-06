from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Count By Key')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_distinct.txt')

rdd2 = rdd.flatMap(lambda x: x.split(' '))

rdd3 = rdd2.countByKey()

print(rdd3.collect())