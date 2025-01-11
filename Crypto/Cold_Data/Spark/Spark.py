from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
import pandas as pd
def WriteToCassandra(dataframe,tables, keyspaces):
    if dataframe.empty:
        print(" ")
    else:
        sparkDF = spark.createDataFrame(dataframe)
        #sparkDF.show()
        sparkDF.write.format("org.apache.spark.sql.cassandra")\
                .options(table=tables,keyspace = keyspaces).save(mode="append")

conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")
sc = SparkContext(conf=conf,appName="StreamingStockDataToCassandra")
spark = SparkSession.builder\
	.master("local[1]").appName("SparkCryptoData").getOrCreate()
data=spark.read.csv('hdfs://127.0.0.1:9000/CrytoData/FlumeData.*.csv')

for i in range(0,len(data.collect())):
    symbol = data.collect()[i][0]
    timess = data.collect()[i][2]
    year = timess[0:4]
    month = timess[5:7]
    day = timess[8:10]
    hour = timess[11:13]
    min = timess[14:16]
    second =timess[17:19]
    currency_base = data.collect()[i][1]
    price = data.collect()[i][3]
    bid = data.collect()[i][4]
    ask = data.collect()[i][5]
    day_volume = data.collect()[i][6]
    dataframe= pd.DataFrame(columns=['symbol','time','year','month','day','hour','min','second','currency_base','price','bid','ask','day_volume'])
    dataframe= dataframe.append({'symbol':symbol,'time':timess,'year':year,'month':month,'day':day,'hour':hour,'min':min,'second':sec,'currency_base':currency_base,'price':price,'bid':bid,'ask':ask,'day_volume':day_volume},ignore_index=True)
    WriteToCassandra(dataframe,"batchdata","cryptodata")

