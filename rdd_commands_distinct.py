from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Distinct')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_distinct.txt')

rdd2 = rdd.flatMap(lambda x: x.split(' '))

rdd3 = rdd2.distinct()

print(rdd3.collect())