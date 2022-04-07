from pyspark import SparkConf, SparkContext, StorageLevel
from typing import List
import numpy as np
from operator import add
# 官方文档
"https://spark.apache.org/docs/2.3.0/api/python/"
# 学习文档
"https://sparkbyexamples.com/pyspark-rdd"
# RDD的创建
"""
方法一：
#Create RDD from parallelize    
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)

方法二：

#Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("/path/textFile.txt")

"""

# 算子的使用

def test_rdd(data: List):
    rdd = sc.parallelize(data)
    print(rdd.collect())


def test_count():
    """
    类似的方法还有sum,mean,max,min,stddev,variance
    :return:
    :rtype:
    """
    print(sc.parallelize([2, 3, 4]).count())


# Python中常见高阶函数
def test_map(data):
    rdd = sc.parallelize(data)
    rdd1 = rdd.map(lambda x: x + 1)
    print(rdd1.collect())


def test_mappar():
    """
    Return a new RDD by applying a function to each partition of this RDD

    """

    rdd = sc.parallelize([1, 2, 3, 4], 2)

    def f(iterator):
        yield sum(iterator)
    print(rdd.collect())
    # glom,以列表的方式展现出来每个分区的数据
    print(rdd.glom().collect())

    print(rdd.mapPartitions(f).collect())
    # 返回的是分区数据，不是分组数据
    # print(rdd.mapPartitions(lambda x: x+1).collect())


def test_filter(data):
    rdd = sc.parallelize(data)
    maprdd = rdd.map(lambda x: x * 2)
    filterrdd = maprdd.filter(lambda x: x > 5)
    print(filterrdd.collect())


def test_reduce():
    # rdd = sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
    rdd = sc.parallelize([1, 2, 3, 4, 5]).reduce(lambda x, y: x+y)
    print(rdd)


def test_distinct():
    print(sc.parallelize([1, 1, 2, 3]).distinct().collect())


# 重点！！！
def test_groupby():
    rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
    print(rdd.groupBy(lambda x: x % 2).collect())
    # 想看一下分组的结果,在Pandas中如何查看分组结果
    print(rdd.groupBy(lambda x: x % 2).map(lambda x: (x[0], [i for i in x[1]])).collect())
    print(rdd.groupBy(lambda x: x % 2).map(lambda x: {x[0]: list(x[1])}).collect())


# 映射
def test_flatmap():
    """
    每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
    先应用映射，再展开
    """
    rdd = sc.parallelize(["hello python", 'hello world', "hello spark"])
    rdd = sc.textFile('./data/test.txt')
    rdd1 = sc.parallelize([np.array([1, 2, 3]), np.array([1, 2, 3])])

    print(rdd.flatMap(lambda x: x.split(" ")).collect())
    print(rdd1.flatMap(lambda x: x + 1).collect())


def test_flatmapvalue():
    rdd = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
    print(rdd.flatMapValues(lambda x: x).collect())


# 键值对操作
def test_countbykey():
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    print(rdd.countByKey())
    # countByValue相当于值计数
    rdd1 = sc.parallelize([1, 2, 1, 2, 2], 2)
    print(rdd1.countByValue())


def test_groupbykey():
    rdd = sc.parallelize(["I like machine learning",
                           "I love deep learning",
                           "I like NLP",
                           "I like spark",
                           "Do you like me?"])
    # groupByKey按照键分组，仅仅分组，分组的结果是成对的数据
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).groupByKey().collect())
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).groupByKey().map(lambda x:[x[0],list(x[1])]).collect())
    # mapValues(func)可以对成对数据的后半部分做运算
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).groupByKey().mapValues(len).collect())


def test_reducebykey():
    rdd = sc.parallelize(["hello world",
                          "hello spark",
                          "spark is wonderful"])
    # reduceByKey按照键规约，也就是聚合，聚合的时候需要指定聚合函数
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add).collect())


def test_sortbykey():
    rdd = sc.parallelize(["hello world",
                          "hello spark",
                          "python spark",
                          "spark is wonderful"])
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y).sortByKey(False).collect())
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1],x[0])).sortByKey().collect())
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[1], x[0])).sortByKey(False).collect())
    print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0])).collect())


def test_sortby():
    rdd = sc.parallelize([('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)])
    print(rdd.sortBy(lambda x: x[0]).collect())
    print(rdd.sortBy(lambda x: x[1], ascending=False).collect())


def test_partition():
    """
    RDD重新分区
    Some times we may need to repartition the RDD, PySpark provides two ways to repartition;
    repartition() method which shuffles data from all nodes also called full shuffle
    coalesce() method which shuffle data from minimum nodes,
    for examples if you have data in 4 partitions and doing coalesce(2) moves data from just 2 nodes.
    """
    rdd = sc.parallelize([i for i in range(10)], 3)
    print(rdd.getNumPartitions())
    print(rdd.repartition(4).getNumPartitions())
    print(rdd.coalesce(2).getNumPartitions())  # 合并分区


def test_persist():
    rdd = sc.parallelize([1,2,3,4])
    """
    self.useDisk = useDisk
    self.useMemory = useMemory
    self.useOffHeap = useOffHeap
    self.deserialized = deserialized
    self.replication = replication
    """
    print(rdd.persist(storageLevel=StorageLevel(False, True, False, False, 1)).is_cached)


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[4]").setAppName("test")
    sc = SparkContext(conf=conf)
    # for i in sc.getConf().getAll():
    #     print(i)
    l = [1, 2, 3, 4, 5, 6]
    # test_rdd(l)
    # test_map(l)
    # test_mappar()
    # test_filter(l)
    # test_groupby()
    # test_reduce()
    # test_flatmap()
    # test_flatmapvalue()
    # test_count()
    # test_distinct()
    # test_groupbykey()
    # test_reducebykey()
    # test_sortbykey()
    # test_sortby()
    # test_partition()
    test_persist()
    sc.stop()
