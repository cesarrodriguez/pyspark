from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Fist Spark App')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample.txt')

rdd2 = rdd.filter(lambda x: x != '31 32 33 34 35')

print(rdd2.collect())