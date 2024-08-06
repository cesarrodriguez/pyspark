from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Fist Spark App')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_words.txt')

rdd2 = rdd.flatMap(lambda x: x.split(' '))

rdd3 = rdd2.filter(lambda x: not (x.startswith('c') or x.startswith('a')))

print(rdd3.collect())