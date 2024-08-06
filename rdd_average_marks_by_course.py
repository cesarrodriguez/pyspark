from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Average Marks Archived By Course')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../files/StudentData.csv')

rdd2 = rdd.filter(lambda x: x.split(',')[3] != 'course')

rdd3 = rdd2.map(lambda x: (x.split(',')[3], (int(x.split(',')[5]), 1)))

print(rdd3.collect())

rdd4 = rdd3.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

rdd5 = rdd4.map(lambda x: (x[0], x[1][0]/x[1][1]))

print(rdd5.collect())
