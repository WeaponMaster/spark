from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import when, expr, add_months
from pyspark.sql import functions as fn
from pyspark.sql.window import Window

def test_when():
	# 方法1 使用pyspark原生方法，实现列的映射
	# df.withColumn('行为类型',
	# 			  when(df.type == 1, "浏览")
	# 			  .when(df.type == 2, "加购")
	# 			  .when(df.type == 3, "删购")
	# 			  .when(df.type == 4, "下单")
	# 			  .when(df.type == 5, "关注")
	# 			  .when(df.type == 6, "点击")
	# 			  .otherwise("不存在的类型")
	# 			  ).show()

	# 方法2 使用spark sql实现列的映射
	# df.createTempView('user_behavior')
	# spark.sql("select *, case when type=1 then '浏览' "+
	# 		  "when type=2 then '加购' " +
	# 		  "when type=3 then '删除' " +
	# 		  "when type=4 then '下单' " +
	# 		  "when type=5 then '关注' " +
	# 		  "when type=6 then '点击' " +
	# 		  "else type end as `行为类型` from user_behavior"
	# 		  ).show()
	# df.groupby('cate').count().show()

	# 同时判断多列
	# df.withColumn('品类行为偏好',
	# 			  when((df.type == 4) & (df.cate == 101), 1)
	# 			  .when((df.type == 1) & (df.cate == 101),2)
	# 			  .when((df.type == 2) & (df.cate == 101),3)
	# 			  .otherwise(4)
	# 			  ).show()
	pass

def test_udf():
	# 使用自定义函数实现列的映射
	def en_code(x):
		if x == 1:
			return '浏览'
		elif x == 2:
			return '加购'
		elif x == 3:
			return '删购'
		elif x == 4:
			return '下单'
		elif x == 5:
			return '关注'
		elif x == 6:
			return '点击'
		else:
			'不存在的行为类型'
	# 必须使用functions下的udf对自定义函数做一层包装，才能使用
	print('------111111------')
	type_udf = fn.udf(en_code, StringType())
	print('-------2222222--------')
	df.withColumn('行为类型', type_udf(df.type)).show(10,False)
	print('-----------333333------------')
	# 如果逻辑比较简单的话可以使用lambda函数
	self_udf = fn.udf(lambda x: 'little' if x < 7 else 'big', StringType())
	df.withColumn('品类', self_udf(df['cate'])).show()

def test_expr():
	# expr提供一个可以在dataframe里实现sql语法的方式
	# df.withColumn('品类', expr("case when cate < 7 then 'small' when cate < 10 then 'mid' else 'big' end")).show()
	# df.withColumn('new_time', expr("add_months(time,1)")).show()
	# df.select(add_months('time',1).alias('new_time')).show()
	# df.withColumn('new_time', add_months('time',1)).show()
	df.withColumn("type_cate", expr(" type ||'_'|| cate")).show()

def test_split():
	# df.select('*',
	# 		  fn.split('time',' ')[0].alias('date'),
	# 		  fn.split('time',' ')[1].alias('time')
	# 		  ).show()
	df.select(fn.split(df['time'],' ')[0].alias('date')).show()
	# print(df.rdd.map(lambda x:(x[0],x[1],x[2].split(' '))).collect())

def test_groupby():
	df1.show()
	# 每天有多少个订单
	# df1.groupby(fn.substring('o_date',1,10).alias('date')).count().show()
	# 每个订单多少个商品
	# df1.groupby('o_id').sum('o_sku_num').alias('sum_sku').show()
	# df1.groupby('o_id').sum('o_sku_num','o_area').alias('sum_sku').show()
	"""
	所有聚合函数
	approx_count_distinct,countDistinct, count
	avg, mean, sum, sumDistinct,max, min
	collect_list, collect_set
	grouping
	first, last
	kurtosis,skewness
	(stddev, stddev_samp), stddev_pop
	(variance, var_samp), var_pop
	"""
	# df1.groupby('o_id').agg({'o_sku_num':'sum'}).show()
	# df1.groupby('o_id').agg(fn.sum('o_sku_num').alias('商品总量'),fn.avg('o_sku_num').alias("商品平均数")).show()
	# 每个订单有哪些商品
	# df1.groupby(fn.substring('o_date',1,10).alias('date')).agg(fn.collect_list('sku_id')).show()
	# 每个日期有哪些不同的sku
	# df1.groupby(fn.substring('o_date',1,10).alias('date')).agg(fn.collect_set('sku_id')).show()
	# df1.groupby(fn.substring('o_date',1,10).alias('date')).agg(fn.collect_set('sku_id')).show()
	# df1.groupby(fn.substring('o_date',1,10).alias('date')).agg(fn.concat_ws(',', 'sku_id')).show()

	# pivot函数,分组字段负责行索引，pivot负责列索引，聚合函数负责值的计算
	# df1.groupby(fn.substring('o_date',1,10)).pivot('o_area').sum('o_sku_num').show()
	# df1.groupby(fn.substring('o_date',1,10), 'o_area').sum('o_sku_num').show()



def test_time():

	# to_timestamp
	# df.withColumn('new_date', fn.to_timestamp('time')).printSchema()
	# df.withColumn('new_date', fn.to_date('time')).printSchema()
	# df.withColumn('new_date', fn.date_format('time','yyyy/mm/dd')).show()
	# 相差天数
	# df.withColumn('days', fn.datediff(fn.current_date(),'time')).show()
	# 相差月数
	# df.withColumn('months', fn.months_between(fn.current_date(),'time')).show()
	# 相差年数
	# df.withColumn('years', fn.months_between(fn.current_date(),'time')/fn.lit(12)).show()
	# add_month
	# df.withColumn('new', fn.add_months('time', 3)).show()
	# df.withColumn('new', fn.date_add('time', 3)).show()
	# df.withColumn('new', fn.date_sub('time', 3)).show()
	# 年月日时分秒
	df.withColumn('new', fn.year('time')).show()
	df.withColumn('new', fn.month('time')).show()
	df.withColumn('new', fn.dayofmonth('time')).show()
	df.withColumn('new', fn.hour('time')).show()
	df.withColumn('new', fn.minute('time')).show()
	df.withColumn('new', fn.second('time')).show()

	df.withColumn('new', fn.dayofweek('time')).show()
	df.withColumn('new', fn.dayofyear('time')).show()
	df.withColumn('new', fn.weekofyear('time')).show()

def test_window():
	df1.withColumn('row_number',fn.row_number().over(Window.partitionBy('o_area').orderBy('o_date'))).show(100)

def test_concat_ws():
	pass


def test_join():
	d1 = [(1, "Smith", -1, "2018", "10", "M", 3000),
		   (2, "Rose", 1, "2010", "20", "M", 4000),
		   (3, "Williams", 1, "2010", "10", "M", 1000),
		   (4, "Jones", 2, "2005", "10", "F", 2000),
		   (5, "Brown", 2, "2010", "40", "", -1),
		   (6, "Brown", 2, "2010", "50", "", -1)
		   ]
	empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
				  "emp_dept_id", "gender", "salary"]

	emp = spark.createDataFrame(data=d1, schema=empColumns)
	# emp.printSchema()
	emp.show(truncate=False)

	d2 = [("Finance", 10),
			("Marketing", 20),
			("Sales", 30),
			("IT", 40)
			]
	deptColumns = ["dept_name", "dept_id"]
	dept = spark.createDataFrame(data=d2, schema=deptColumns)
	# dept.printSchema()
	dept.show(truncate=False)

	# emp.join(dept,emp.emp_dept_id==dept.dept_id, how='inner').show()
	# emp.join(dept,emp.emp_dept_id==dept.dept_id, how='outer').show()
	emp.join(dept,emp.emp_dept_id==dept.dept_id, how='left').show()
	emp.join(dept,emp.emp_dept_id==dept.dept_id, how='leftsemi').show()


if __name__ == '__main__':
	spark = SparkSession.builder.master("local[4]").appName("builtin").getOrCreate()
	df = spark.read.parquet('E:/spark/data/action_part.parquet')
	# test_when()
	# test_udf()
	# test_expr()
	# test_split()

	# df1 = spark.read.csv('E:/spark/data/order.csv',inferSchema=True,header=True)
	# test_groupby()
	# test_time()
	# test_window()

	user = spark.read.csv('C:/Users/Master//PycharmProjects/spark/data/user.csv',encoding='gbk',header=True,inferSchema=True)
	test_join()