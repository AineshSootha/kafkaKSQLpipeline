from kafka import KafkaConsumer
from json import loads
from time import sleep
import MySQLdb as sql

#db = sql.connect(host ='localhost', user = "root", passwd = "password", db ="pythonTest")


consumer = KafkaConsumer(
    'topic_test',
    bootstrap_servers='kafka:9093',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for event in consumer:
   # event_data = event.value
    print(event)