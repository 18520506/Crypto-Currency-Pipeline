import pyspark
from pyspark import SparkContext,SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import *
import pandas as pd
import requests
import json





def WriteToCassandra(dataframe,tables,keyspaces):
    if dataframe.empty:
        print("FALSE")
    else:
        sparkDF = spark.createDataFrame(dataframe)
        sparkDF.write.format("org.apache.spark.sql.cassandra")\
            .options(table=tables,keyspace=keyspaces).save(mode="append")


def getMessage(message):
   
    records = message.collect()
    if len(records)== 1:
        content = records[0][1].split(",")
        symbol = content[0]
        currency_base = content[1]
        timestamp_converted = content[2]
        price = content[3]
        bid = content[4]
        ask = content[5]
        day_volume = content[6]
        year = timestamp_converted[0:4]
        month = timestamp_converted[5:7]
        day = timestamp_converted[8:10]
        hour = timestamp_converted[11:13]
        min = timestamp_converted[14:16]
        sec = timestamp_converted[17:19]

        if symbol =="BTC/USD":
            with open('/Crypto/Hot_Data/apiPowerBi.txt','r') as f:
                link = f.readlines()
        else:
            with open("/Crypto/Hot_Data/APIETH.txt",'r') as f2:
                link = f2.readlines()
        url = str(link[0])
        dataframe= pd.DataFrame(columns=['symbol','time','year','month','day','hour','min','second','currency_base','price','bid','ask','day_volume'])
        dataframe= dataframe.append({'symbol':symbol,'time':timestamp_converted,'year':year,'month':month,'day':day,'hour':hour,'min':min,'second':sec,'currency_base':currency_base,'price':price,'bid':bid,'ask':ask,'day_volume':day_volume},ignore_index=True)
        #WriteToCassandra(dataframe, "streaming","cryptodata")
        data = {'symbol':symbol,'time':timestamp_converted,'year':year,'month':month,'day':day,'hour':hour,'min':min,'second':sec,'currency_base':currency_base,'price':price,'bid':bid,'ask':ask,'day_volume':day_volume}
        headers = {"Content-Type": "application/json"}
        response = requests.request(method="POST",url=url,headers=headers,data=json.dumps(data))
        #time_lib.sleep(20)
                



if __name__=="__main__":
    conf= SparkConf().set("spark.cassandra.connection.host","127.0.0.1").set("spark.sql.excution.arrow.pyspark.enabled","true")
    sc = SparkContext(conf = conf, appName = "CryptoStreaming")
    ssc = StreamingContext(sc,1)
    sql = SQLContext(sc)
    spark = SparkSession.builder\
        .master("local[1]") \
            .getOrCreate()
    zkQuorum = 'localhost:9092'
    topic = ["CryptoData"]
    kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list":zkQuorum})
    kvs.foreachRDD(getMessage)
    ssc.start()
    ssc.awaitTermination()
