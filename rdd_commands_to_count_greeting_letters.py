from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Fist Spark App')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_greeting.txt')

rdd2 = rdd.map(lambda x: x.split(' '))

rdd3 = rdd2.map(lambda x: [len(y) for y in x])
    
print(rdd3.collect())