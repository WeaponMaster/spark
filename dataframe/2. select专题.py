from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


def test_select():
    # 构造数据
    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]
    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data=data, schema=columns)

    # 直接写列名
    # df.select("firstname").show()
    # df.select("firstname", "lastname").show()
    # df.select(['firstname', 'lastname']).show()
    # df.select(df['firstname'], df['lastname']).show()
    # df.select(df.firstname, df.lastname).show()
    # 语义重复的情况
    # df.select(df[['firstname','lastname']]).show()
    # 使用col函数
    df.select(F.col('firstname')).show()
    # 正则表达式获取列
    # df.select(df.colRegex("`.*name*`")).show()
    # 选择所有列
    # df.select(*columns).show()
    # df.select("*").show()
    # df.select([col for col in df.columns]).show()
    # 通过索引号来取列
    # df.select(df.columns[0]).show()
    # df.select(df.columns[:3]).show()
    # df.select(df.columns[1:3]).show()
    # 不支持花式索引
    # df.select(df.columns[0, 3]).show()
    # 不支持按rows取数据
    # df.select(df.rows[:3]).show()

def test_complex():
    # 构造嵌套结构数据
    data = [(("James", None, "Smith"), "OH", "M"),
            (("Anna", "Rose", ""), "NY", "F"),
            (("Julia", "", "Williams"), "OH", "F"),
            (("Maria", "Anne", "Jones"), "NY", "M"),
            (("Jen", "Mary", "Brown"), "NY", "M"),
            (("Mike", "Mary", "Williams"), "OH", "M")
            ]
    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
    ])
    df = spark.createDataFrame(data, schema=schema)
    # df.printSchema()
    # df.show()
    df.select('name').show()
    df.select('name.firstname', 'name.lastname').show()
    df.select('name.firstname', 'name.middlename', 'name.lastname').show()
    df.select('name.*').show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ex').getOrCreate()
    # test_select()
    test_complex()

