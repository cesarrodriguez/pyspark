from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Save as text file')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/sample_distinct.txt')

rdd2 = rdd.flatMap(lambda x: x.split(' '))

rdd3 = rdd2.map(lambda x: (x, 1))

print(rdd3.collect())

rdd3.saveAsTextFile('../txt/output/sample_distinct')