from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Min and Max By City')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../txt/average_quiz_sample.csv')

rdd2 = rdd.map(lambda x: (x.split(',')[1],float(x.split(',')[2])))

rdd3 = rdd2.reduceByKey(lambda x,y: x if x < y else y)

print(rdd3.collect())

rdd4 = rdd2.reduceByKey(lambda x,y: x if x > y else y)

print(rdd4.collect())

