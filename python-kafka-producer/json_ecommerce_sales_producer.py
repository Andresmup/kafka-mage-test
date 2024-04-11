import pandas as pd
import json
import os
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092',])
topic = 'ecommerce'

df_ecommerce_sale_report = pd.read_csv("ecommerce_sale_report.csv", index_col="index",low_memory=False)
df_ecommerce_sample = df_ecommerce_sale_report.sample(n=10000, random_state=42)

for index, row in df_ecommerce_sample.iterrows():
    data = {}
    for column, value in row.items():
        data[column] = value
        
    producer.send(topic, json.dumps(data).encode('utf-8'))
    time.sleep(0.1)