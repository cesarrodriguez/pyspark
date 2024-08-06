from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Total Marks Archived By Course')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../files/StudentData.csv')

rdd2 = rdd.map(lambda x: (x.split(',')[3], x.split(',')[5]))

rdd3 = rdd2.filter(lambda x: x[0] != 'course')

rdd4 = rdd3.reduceByKey(lambda x,y: int(x)+int(y))

print(rdd4.collect())