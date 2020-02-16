from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()\
    .setAll(
        [
            ('spark.driver.host', 'localhost[*]'),
            ('spark.driver.port',6294),
            ('spark.driver.bindAddress', '0.0.0.0')
    ])

conf.setMaster('spark://192.168.0.243:7077')
conf.setAppName('spark-basic')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')

def mod(x):
    import numpy as np
    return (x, np.mod(x, 2))

rdd = sc.parallelize(range(1000)).map(mod).take(10)
print(rdd)