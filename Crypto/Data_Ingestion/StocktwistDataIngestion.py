from kafka import KafkaProducer
import json
import time 
import requests 
from time import gmtime, strftime


def stream_symbol(symbol):        
        url = "https://api.stocktwits.com/api/2/streams/symbol/" + str(symbol) + ".json"
        try:
            content = requests.get(url).text
        except Exception as e:
            print(0)
        return json.loads(content)


symbol_list = ['BTC.X',"ETH.X"]
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
for symbol in symbol_list:
    res = stream_symbol(symbol)
    content = json.dumps(res)
    content = content.lower()
    bearish_count = content.count("bear")
    bullrish_count = content.count('bull')
    times = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    messages = str(symbol)+ "," + str(bearish_count) + ","+ str(bullrish_count)+","+str(times)
    producer.send("CryptoSentiment",messages.encode(encoding='UTF-8'))
    print(messages)
    time.sleep(2)
