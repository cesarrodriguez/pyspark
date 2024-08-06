from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Fist Spark App')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample.txt')

rdd2 = rdd.map(lambda x: x.split(' '))

print(rdd2.collect())