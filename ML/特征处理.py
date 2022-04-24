from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.feature import VectorAssembler, StandardScaler



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



if __name__ == '__main__':
    spark = SparkSession.builder.master('local[4]').appName('feature').getOrCreate()
    df = spark.read.json('E:/spark/data/zipcodes.json')
    # df.printSchema()
    # df.show()
    # test_string_indexer()
    # test_onehot()
    # test_nlp()
    # test_vector()
    test_stdscaler()