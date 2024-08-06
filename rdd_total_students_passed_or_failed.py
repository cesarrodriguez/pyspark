from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Passed or Failed')

sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('../files/StudentData.csv')

rdd2 = rdd.map(lambda x: (x.split(',')[2], x.split(',')[5]))

rdd3 = rdd2.filter(lambda x: x[1] != 'marks')

rdd4 = rdd3.filter(lambda x: int(x[1]) < 50)

rdd5 = rdd3.filter(lambda x: int(x[1]) >= 50)

print('Number of students Failed: ' + str(rdd4.count()) + ' and number of students Passed: ' + str(rdd5.count()))