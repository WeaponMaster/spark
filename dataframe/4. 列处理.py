"""
PySpark withColumn() is a transformation function of DataFrame which is used to change the value,
convert the datatype of an existing column, create a new column, and many more.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn


def test_col_idx():
	"""
	 列索引
	"""
	l = [("James", 23), ("Ann", 40)]
	df1 = spark.createDataFrame(l).toDF("name","age")
	df1.printSchema()
	df1.show()
	# 方式一
	df1.select(df.age).show()
	df1.select(df["age"]).show()
	df1.select(df["name"]).show()
	# 方式二
	df1.select(fn.col("age")).show()
	df1.select(fn.col("name")).show()


def test_col_op():
	"""
	列方法,列操作
	"""

	l = [(100, 2, 1), (200, 3, 4), (300, 4, 4)]
	df1 = spark.createDataFrame(l).toDF("col1", "col2", "col3")

	# 数学运算
	# df1.select(df1.col1 + df1.col2).show()
	# df1.select(df1.col1 - df1.col2).show()
	# df1.select(df1.col1 * df1.col2).show()
	# df1.select(df1.col1 / df1.col2).show()
	# df1.select(df1.col1 % df1.col2).show()

	# 布尔运算
	# df1.select(df1.col2 > df1.col3).show()
	# df1.select(df1.col2 < df1.col3).show()
	# df1.select(df1.col2 == df1.col3).show()

def test_alias():
	# 别名
	l = [("James", "Bond", "100", None),
	        ("Ann", "Varsa", "200", 'F'),
	        ("Tom Cruise", "XXX", "400", ''),
	        ("Tom Brand", None, "400", 'M')]
	cols = ["fname", "lname", "id", "gender"]
	df1 = spark.createDataFrame(data, columns)
	# alias
	# df1.select(df1.fname.alias("first_name"), df1.lname.alias("last_name")).show()
	# 转换类型
	# df1.select(df1.fname, df1.id.cast("int")).printSchema()
	# 排序
	# df1.sort(df1.id.asc()).show()
	# df1.sort(df1.id.desc()).show()

def test_filter():
	# filter
	# between
	# df.filter(df.id.between(100, 300)).show()
	# contains
	# df.filter(df['fname'].contains('Tom')).show()
	# startwith endwith
	# df.filter(df['fname'].startswith('T')).show()
	# isNull notNull
	# df.filter(df['gender'].isNull()).show()
	# df.filter(df['gender'].isNotNull()).show()
	# like
	# df.filter(df['lname'].like('B%')).show()
	# isin
	# df.filter(df['id'].isin(['100', '200'])).show()
	pass


def test_datatype():
	print(df.printSchema())
	# 列类型转换
	df.withColumn("sal", F.col("salary").cast("String"))
	print(df.printSchema()) # 原数据不会更改
	# 创建一个新列
	newdf = df.withColumn("sal", F.col("salary").cast("String"))
	print(newdf.printSchema())


def test_stat():
	df.select(fn.max(zipcode['population'])).show()
	df.select(fn.min(zipcode['population'])).show()


def test_update():
	# df.show()
	# 列操作
	# df.withColumn('salary', F.col('salary')*100).show()
	# 重命名
	# df.withColumnRenamed('gender', 'sex').show()
	# df.withColumnRenamed('gender', 'sex').withColumnRenamed('salary','sal').show()
	# df.toDF(*['col_{}'.format(i) for i in range(len(df.columns))]).show()
	# 删除列,预览模式
	# df.drop('middlename').show()
	df.show()

def test_map():
	df.show()
	def func(x):
		name = x.firstname+','+x.lastname
		gender = x.gender.lower()
		salary = x.salary*2
		return name,gender,salary
	# 重要: map返回值是 Dataset[U],而不是DataFrame
	# df.rdd.map(lambda x:func(x)).toDF(['name','gender','salary']).show()
	# df.rdd.map(lambda x: (x[0]+','+x[1], x[2], x[3])).toDF(['name','gender','salary']).show()
	df.rdd.map(lambda x: (x['firstname']+','+x['lastname'], x['gender'], x['salary'])).toDF(['name','gender','salary']).show()


def test_flatmap():
	# PySpark DataFame doesn’t have flatMap() transformation
	# nestdf.select(nestdf.name, F.explode(nestdf['languages']).alias('course')).show()
	# nestdf.select(nestdf.name, F.explode(nestdf['properties'])).show()
	# nestdf.select(nestdf.name, F.explode(nestdf['properties']).alias('body','color')).show()
	# nestdf.select(nestdf.name, F.explode(nestdf['properties']).alias(*['body','color'])).show()
	# nestdf.select(nestdf.name, F.explode(nestdf['languages']), F.explode(nestdf['properties']).alias(*['body','color'])).show()
	# explode_outer会把空值创建出来
	# nestdf.select(nestdf.name, F.explode_outer(nestdf['languages'])).show()
	# poseexplode会对炸裂之后的数据加一列索引
	# nestdf.select(nestdf.name, F.posexplode(nestdf['languages'])).show()
	# nestdf.select(nestdf.name, F.posexplode(nestdf['properties'])).show()
	# poseexplode_outer
	nestdf.select(nestdf.name, F.posexplode_outer(nestdf['languages'])).show()

def test_na():
	zipcode.printSchema()
	# 填充空值
	# zipcode.fillna(value=0,subset=["population"]).show()
	# zipcode.na.fill(value="",subset=["city"]).show()
	# zipcode.fillna({"type":"", "population":0}).show()
	# 删除空值
	# zipcode.dropna(how='any').show()
	# zipcode.dropna(how='all').show()
	zipcode.dropna(subset='city').show()

if __name__ == '__main__':
	spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

	data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
			('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
			('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
			('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
			('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]
	columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
	df = spark.createDataFrame(data=data, schema=columns)
	# test_datatype()
	# test_update()
	# test_map()
	nestdata = [
		('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
		('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
		('Robert', ['CSharp', 'PHP'], {'hair': 'red', 'eye': ''}),
		('Washington', None, None),
		('Jefferson', ['Python', 'C++'], {})]
	nestdf = spark.createDataFrame(data=nestdata, schema=['name', 'languages', 'properties'])
	# test_flatmap()
	zipcode = spark.read.csv('./data/small_zipcode.csv',header=True,inferSchema=True)
	# test_na()
	test_stat()
	spark.stop()
