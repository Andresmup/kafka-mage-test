import pandas as pd
import json
import os
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092',])
topic = 'opensearch'

df_ecommerce_sale_report = pd.read_csv("ecommerce_sale_report.csv", index_col="index",low_memory=False)
df_ecommerce_sample = df_ecommerce_sale_report.sample(n=10, random_state=44)

for index, row in df_ecommerce_sample.iterrows():
    data = {}
    for column, value in row.items():
        if pd.isna(value):  # Convertir NaN a None
            value = None
        elif column == 'ship-postal-code':  # Convertir ship-postal-code a cadena
            value = str(value)
        data[column] = value
        
    producer.send(topic, json.dumps(data).encode('utf-8'))
    time.sleep(0.1)