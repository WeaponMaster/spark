from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, LongType, IntegerType, FloatType, BooleanType
from pyspark.sql import functions as fn
import pandas as pd
import numpy as np

def test_todf():
    # 方式一：使用toDF()把RDD转化成DataFrame
    columns = ["language","users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    rdd = spark.sparkContext.parallelize(data)
    print(rdd.collect())
    df = rdd.toDF(schema=columns)
    df.printSchema()
    df.show()


def test_creat():
    # 方式二：使用createDataFrame,每列数据类型必须一致
    data = [("James", "", "Smith", 30, "M", 60000.0),
            ("Michael", "Rose", "", 50, "M", 70000.0),
            ("Robert", "", "Williams", 42, "", 400000.0),
            ("Maria", "Anne", "Jones", 38, "F", 500000.0),
            ("Jen", "Mary", "Brown", 45, "F", 0.0)]

    columns = ["first_name", "middle_name", "last_name", "Age", "gender", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    # spark会自动推断数据类型
    df.printSchema()
    df.show()
    """
    When you infer the schema, by default the datatype of the columns is derived from the data and set’s nullable to true 
    for all columns.We can change this behavior by supplying schema using StructType – where we can specify a column name, 
    data type and nullable for each field/column.
    """
    # 手动指定类型
    dfschema = StructType([StructField('first_name', StringType(), True),
                           StructField('middle_name', StringType(), True),
                           StructField("last_name", StringType(), True),
                           StructField("age", IntegerType(), True),
                           StructField("gender", StringType(), True),
                           StructField("salary", FloatType(), True)])
    df1 = spark.createDataFrame(data, schema=dfschema)
    df1.printSchema()
    df1.show()


def test_read_csv():
    # 读取csv
    df = spark.read.csv("./data/zipcodes.csv", header=True)
    df.printSchema()
    df.show(5)
    # 转变成pandas
    data = df.toPandas()
    print(data.columns)


def test_json():
    # df = spark.read.json('C:/Users/Master/PycharmProjects/spark/data/zipcodes.json')
    # df.printSchema()
    # df.show()
    # df = spark.read.json(['C:/Users/Master/PycharmProjects/spark/data/zipcode1.json',
    #                      'C:/Users/Master/PycharmProjects/spark/data/zipcode2.json'])
    # df.show()
    # df = spark.read.option('multiline','true').json('C:/Users/Master/PycharmProjects/spark/data/multiline-zipcode.json')
    # df.show()
    # 使用SQL读取json
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW zipcode USING json OPTIONS (path 'data/zipcodes.json')")
    spark.sql("select * from zipcode").show()

def test_parquet():
    df = spark.read.csv('C:/Users/Master/PycharmProjects/spark/data/actions.csv',header=True,inferSchema=True)
    df.write.parquet('C:/Users/Master/PycharmProjects/spark/data/actions.parquet')
    df1 = spark.read.csv('C:/Users/Master/PycharmProjects/spark/data/actions1.csv',header=True,inferSchema=True)
    df1.write.mode('append').parquet('C:/Users/Master/PycharmProjects/spark/data/actions.parquet')
    # 使用partitionBy分区保存
    # pardf = spark.read.parquet('C:/Users/Master/PycharmProjects/spark/data/actions.parquet')
    # pardf.show()
    # 创建一个新列
    # newdf = pardf.withColumn('date', fn.split('time',' ')[0])
    # newdf.show()
    # 分区保存
    # newdf.write.partitionBy("date").mode("overwrite").parquet("C:/Users/Master/PycharmProjects/spark/data/action_part.parquet")
    # 读取分区文件
    spark.read.parquet('C:/Users/Master/PycharmProjects/spark/data/action_part.parquet/date=2016-03-15').show()

def test_jdbc():
    """
    https://dev.mysql.com/downloads/connector/j/
    :return:
    """
    prop = {
        'user': 'root',
        'password': '123456',
        'driver': 'com.mysql.cj.jdbc.Driver'
            }
    data = spark.read.jdbc(url='jdbc:mysql://localhost:3306/train?serverTimezone=UTC',table='orders',properties=prop)
    data.show()

    data.write.jdbc(url='jdbc:mysql://localhost:3306/train?serverTimezone=UTC',table = 'order_temp',properties=prop,mode='overwrite')

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[2]').appName('df').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # test_todf()
    # test_creat()
    # test_csv()
    # test_json()
    # test_parquet()
    test_jdbc()