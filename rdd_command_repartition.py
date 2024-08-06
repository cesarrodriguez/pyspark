from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Repartition')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_distinct.txt')

rdd = rdd.repartition(5)

rdd2 = rdd.flatMap(lambda x: x.split(' '))

rdd3 = rdd2.map(lambda x: (x, 1))

print(rdd3.collect())

rdd3.saveAsTextFile('../txt/output/5repatition_sample_distinct')