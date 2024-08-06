from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Group By Key')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_distinct.txt')

rdd2 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, len(x)))


rdd3 = rdd2.groupByKey().mapValues(list)

print(rdd3.collect())