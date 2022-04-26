from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.types import StructType,StructField,StringType,DoubleType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def test_string_indexer():
    df.groupby('State').count().show()
    model = StringIndexer(inputCol='State',outputCol='new_state',stringOrderType='frequencyDesc')
    indexer = model.fit(df)
    index =indexer.transform(df)
    index.show()

def test_onehot():
    # 用之前，先把字符串转成数字类型
    indexer = StringIndexer(inputCol='State',outputCol='new_state').fit(df).transform(df)
    onehot = OneHotEncoderEstimator(inputCols=['new_state'],outputCols=['state_onehot'])
    onehot.fit(indexer).transform(indexer).show()

def test_nlp():
    token = Tokenizer(inputCol='LocationText',outputCol='location',).transform(df)
    remove = StopWordsRemover(inputCol='location',outputCol='remove').transform(token)
    tf = HashingTF(inputCol='remove',outputCol='tf').transform(remove)
    idf = IDF(inputCol='tf',outputCol='idf').fit(tf).transform(tf)
    idf.show()

def test_vector():
    vector = VectorAssembler(inputCols=['Xaxis','Yaxis','Zaxis'],outputCol='axes').transform(df)
    vector.show()
def test_stdscaler():
    vector = VectorAssembler(inputCols=['Xaxis','Yaxis','Zaxis'],outputCol='axes').transform(df)
    scaled = StandardScaler(inputCol='axes',outputCol='axes_std_scaled',withMean=True).fit(vector).transform(vector)
    scaled.show()

def test_ml():
    # iris.describe().show()
    # 转变variety的的类型
    iris1 = StringIndexer(inputCol='variety',outputCol='label').fit(iris).transform(iris)
    # iris1.show()
    # 特征向量化
    assembler = VectorAssembler(inputCols=["sepal_length",'sepal_width','petal_length','petal_width'],outputCol='features')
    features = assembler.transform(iris1)
    # features.show()
    train,test = features.randomSplit([0.8,0.2],seed=42)
    # train.show()
    lr = LogisticRegression()
    model = lr.fit(train)
    predict = model.transform(test)
    # predict.printSchema()
    # predict.select('sepal_length','sepal_width','petal_length','petal_width','label','prediction').show()
    evaluator = MulticlassClassificationEvaluator(metricName='f1')
    print(evaluator.evaluate(predict))

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[4]').appName('feature').getOrCreate()
    df = spark.read.json('E:/spark/data/zipcodes.json')
    schema = StructType([StructField('sepal_length',DoubleType(), True),
                        StructField('sepal_width', DoubleType(), True),
                        StructField('petal_length', DoubleType(), True),
                        StructField('petal_width',DoubleType(), True),
                        StructField('variety',StringType(),True)
                        ])
    iris = spark.read.csv(path='E:/spark/data/iris.csv',header=True, schema=schema)
    # iris.show()
    # iris.printSchema()
    # df.printSchema()
    # df.show()
    # test_string_indexer()
    # test_onehot()
    # test_nlp()
    # test_vector()
    # test_stdscaler()
    test_ml()