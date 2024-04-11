from kafka import KafkaProducer
from random import random
import json
import time
from faker import Faker
fake = Faker()


producer = KafkaProducer(bootstrap_servers=['localhost:9092',])
topic = 'test'

for _ in range(100):
    test_title = fake.name()
    director = fake.name()
    date = fake.date_between_dates(date_start=datetime(2015,1,1), date_end=datetime(2019,12,31)).year
    data = {
        'title': test_title,
        'director': director,
        'year': date,
        'rating': random(),
    }
    producer.send(topic, json.dumps(data).encode('utf-8'))
time.sleep(20)
