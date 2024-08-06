from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Coalesce')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_distinct.txt')

rdd = rdd.coalesce(1)

rdd2 = rdd.flatMap(lambda x: x.split(' '))

rdd3 = rdd2.map(lambda x: (x, 1))

print(rdd3.collect())

rdd3.saveAsTextFile('../txt/output/1repatition_sample_distinct')