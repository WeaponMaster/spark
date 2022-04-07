from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import expr


def test_col_idx():
	"""
	 列索引
	"""
	data = [("James", 23), ("Ann", 40)]
	df = spark.createDataFrame(data).toDF("name","age")
	df.printSchema()
	df.show()
	# 方式一
	df.select(df.age).show()
	df.select(df["age"]).show()
	df.select(df["name"]).show()
	# 方式二
	df.select(col("age")).show()
	df.select(col("name")).show()


def test_col_op():
	"""
	列方法,列操作
	"""

	data = [(100, 2, 1), (200, 3, 4), (300, 4, 4)]
	df = spark.createDataFrame(data).toDF("col1", "col2", "col3")

	# 数学运算
	# df.select(df.col1 + df.col2).show()
	# df.select(df.col1 - df.col2).show()
	# df.select(df.col1 * df.col2).show()
	# df.select(df.col1 / df.col2).show()
	# df.select(df.col1 % df.col2).show()

	# 布尔运算
	# df.select(df.col2 > df.col3).show()
	# df.select(df.col2 < df.col3).show()
	# df.select(df.col2 == df.col3).show()

	# 别名
	data = [("James","Bond","100",None),
			("Ann","Varsa","200",'F'),
			("Tom Cruise","XXX","400",''),
			("Tom Brand",None,"400",'M')]
	columns = ["fname","lname","id","gender"]
	df = spark.createDataFrame(data, columns)

	# alias
	# df.select(df.fname.alias("first_name"), df.lname.alias("last_name")).show()
	# 排序
	df.id.asc().show()


if __name__ == '__main__':
	spark = SparkSession.builder.master('local[2]').appName('column').getOrCreate()
	# test_col_idx()
	test_col_op()
	spark.stop()
