
"""
SparkContext和SparkSession有什么区别
https://sparkbyexamples.com/pyspark/pyspark-what-is-sparksession/
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('ss') \
                    .getOrCreate()


rdd = spark.sparkContext.parallelize([1,2,3])
print(rdd.collect())
spark.stop()
print("-----------------------------------------------")

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local[2]").setAppName("sc")
sc = SparkContext(conf=conf)
rdd = sc.parallelize([1,2,3,4,5],numSlices=2)
print(rdd.collect())
sc.stop()