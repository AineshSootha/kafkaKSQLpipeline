import os
import json
import MySQLdb as sql
import kafka
from time import sleep
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

db = sql.connect(host ='localhost', user = "root", passwd = "password", db ="pythonTest")
cursor = db.cursor()
number_of_rows = cursor.execute("select * from transactions")

result = cursor.fetchall()

for i in result:
    jd = json.dumps(i)
    producer.send('TestTopic1', value=jd)
    sleep(0.5)


db.close()
