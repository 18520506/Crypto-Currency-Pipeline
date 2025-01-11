from matplotlib import ticker
from twelvedata import TDClient
import kafka
from kafka import KafkaProducer
import pandas as pd
import json
from datetime import datetime


api_key ='1960b1317a4d4ca381e2dad009d99658'
# with open('/home/binhnguyen2/Videos/twelve_data_apikey.txt','r') as f:
#     api_key = f.readline()
def on_event(event):
    producer = KafkaProducer(bootstrap_servers ='localhost:9092')    
    data = event
    if data['event']=="price":
        symbol = data['symbol']
        currency_base = data['currency_base']
        timestamp = data['timestamp']
        time_stamp_converted = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        price = data['price']
        bid = data['bid']
        ask = data['ask']
        day_volume = data['day_volume']
        messages = str(symbol)+ ","+str(currency_base)+","+str(time_stamp_converted)+","+str(price)+ ","+str(bid)+","+str(ask)+","+str(day_volume)
        producer.send('CryptoData',messages.encode(encoding='UTF-8'))
        print(messages)



symbol_list = ['ETH/BTC','BTC/USD']
td = TDClient(apikey=api_key)
ws = td.websocket(symbols=symbol_list, on_event=on_event)
ws.connect()
ws.keep_alive()
