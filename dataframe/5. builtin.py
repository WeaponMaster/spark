from pyspark.sql import SparkSession
from pyspark.sql.functions import when, expr, add_months
from pyspark.sql import functions as fn

def test_when():
	# 使用pyspark原生方法
	# df.withColumn('行为类型',
	# 			  when(df.type == 1, "浏览")
	# 			  .when(df.type == 2, "加购")
	# 			  .when(df.type == 3, "删购")
	# 			  .when(df.type == 4, "下单")
	# 			  .when(df.type == 5, "关注")
	# 			  .when(df.type == 6, "点击")
	# 			  .otherwise("不存在的类型")
	# 			  ).show()

	# 使用spark sql
	# df.createTempView('user_behavior')
	# spark.sql("select *, case when type=1 then '浏览' "+
	# 		  "when type=2 then '加购' " +
	# 		  "when type=3 then '删除' " +
	# 		  "when type=4 then '下单' " +
	# 		  "when type=5 then '关注' " +
	# 		  "when type=6 then '点击' " +
	# 		  "else type end as `行为类型` from user_behavior"
	# 		  ).show()
	df.groupby('cate').count().show()

	# 同时判断多列
	# df.withColumn('品类行为偏好',
	# 			  when((df.type == 4) & (df.cate == 101), 1)
	# 			  .when((df.type == 1) & (df.cate == 101),2)
	# 			  .when((df.type == 2) & (df.cate == 101),3)
	# 			  .otherwise(4)
	# 			  ).show()
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
	# df.groupby(fn.substring('time',1,10).alias('date')).count().show()
	# df.groupby(fn.substring('time',1,10).alias('date')).sum('brand').show()
	df.groupby(fn.substring('time',1,10).alias('date')).agg(fn.collect_list('user_id')).show()
	# df.groupby(fn.substring('time',1,10).alias('date')).apply(lambda x:','.join(x)).show()
	# fn.substring()
def test_concat_ws():
	pass

if __name__ == '__main__':
	spark = SparkSession.builder.master("local[4]").appName("builtin").getOrCreate()
	df = spark.read.parquet('C:/Users/Master/PycharmProjects/spark/data/actions.parquet')
	# df.printSchema()
	# test_when()
	# test_expr()
	# test_split()
	test_groupby()