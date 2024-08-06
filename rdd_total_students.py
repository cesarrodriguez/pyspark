from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Total Students')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../files/StudentData.csv')

print('Total students are: ' + str(rdd.count()))