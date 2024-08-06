from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Fist Spark App')

sc = SparkContext.getOrCreate(conf = conf)

text = sc.textFile('../txt/sample.txt')

print(text.collect())