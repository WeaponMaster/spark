from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, LongType, IntegerType, FloatType, BooleanType
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



if __name__ == '__main__':
    spark = SparkSession.builder.master('local[2]').appName('df').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    # test_todf()
    # test_creat()
    # test_read_csv()
