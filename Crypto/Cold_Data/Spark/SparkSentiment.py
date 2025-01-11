import pyspark 
from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.conf import SparkConf
import pandas as pd
def WriteToCassandra(dataframe,tables, keyspaces):
    if dataframe.empty:
        print(" ")
    else:
        sparkDF = spark.createDataFrame(dataframe)
        sparkDF.write.format("org.apache.spark.sql.cassandra")\
                .options(table=tables,keyspace = keyspaces).save(mode="append")


conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")
sc = SparkContext(conf=conf,appName="StreamingStockDataToCassandra")
spark = SparkSession.builder\
	.master("local[1]").appName("SparkSentimentData").getOrCreate()
data=spark.read.csv('hdfs://127.0.0.1:9000/StockTwist/FlumeData.*.csv')
das = data.collect()
for i in range(0,len(data.collect())):
    symbol = data.collect()[i][0]
    bearish_count = data.collect()[i][1]
    bullrish_count = data.collect()[i][2]
    dtime = data.collect()[i][3]
    dataframe= pd.DataFrame(columns=['symbol','bearish_count','bullish_count','dtime'])
    dataframe = dataframe.append({'symbol':symbol,'bearish_count':bearish_count,'bullish_count':bullish_count,'dtime':dtime},ignore_index=True)
    WriteToCassandra(dataframe,"data","sentiment")
