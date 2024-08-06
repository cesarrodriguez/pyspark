from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('MAX and MIN Marks Archived By Course')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../files/StudentData.csv')

rdd2 = rdd.filter(lambda x: x.split(',')[3] != 'course')

rdd3 = rdd2.map(lambda x: (x.split(',')[3], int(x.split(',')[5])))

rdd4 = rdd3.reduceByKey(lambda x,y: x if x < y else y)

print(rdd4.collect())

rdd5 = rdd3.reduceByKey(lambda x,y: x if x > y else y)

print(rdd5.collect())
