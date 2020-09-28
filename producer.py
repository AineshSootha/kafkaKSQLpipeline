import os
import io
import json
import logging
import MySQLdb as sql
import MySQLdb.cursors
#import kafka
from time import sleep
from kafka import KafkaProducer
from kafka import KafkaClient
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from kafka.errors import KafkaError
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv

reader = csv.DictReader(open('../customers.csv'))



schema = avro.schema.parse(json.dumps({
    "namespace": "customers.avro",
    "type": "record",
    "name": "pythonTest",
    "fields": [
        {"name" : "id", "type": "string"},
        {"name" : "first_name", "type": "string"},
        {"name" : "last_name", "type": "string"},
        {"name" : "email", "type": "string"},
        {"name" : "gender", "type": "string"},
        {"name" : "club_status", "type": "string"},
        {"name" : "comments", "type": "string"}
    ]
}))
reader_schema = avro.schema.parse(json.dumps({
    "namespace": "customers.avro",
    "type": "record",
    "name": "pythonTest",
    "fields": [
        {"name" : "id", "type": "string"},
        {"name" : "first_name", "type": "string"},
        {"name" : "last_name", "type": "string"},
        {"name" : "email", "type": "string"},
        {"name" : "gender", "type": "string"},
        {"name" : "club_status", "type": "string"},
        {"name" : "comments", "type": "string"}
    ]
}))


# avroProducer = Producer({
#     'bootstrap.servers': 'kafka:9092',
#     #'on_delivery': delivery_report
#     #'schema.registry.url': 'http://schema_registry_host:port'
#     }#, default_key_schema=key_schema, default_value_schema=value_schema)

# for i in reader:
#     avroProducer.produce(topic='test_topic', value = dict(i))

# avroProducer.flush()
#producer = SimpleProducer(client)
#topic = "topic_test"
writer = DataFileWriter(open("transactions.avro", "wb"), DatumWriter(), schema)

#writer.append({"id": "1000", "first_name": "Ainesh", "last_name": "Sootha", "email": "ainesh", "gender": "M", "club_status": "as","comments": "nao"})

producer = KafkaProducer(
    bootstrap_servers='kafka:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#db = sql.connect(host ='172.24.0.2', port = 3306, user ='root', passwd = 'password', db ='newPython', cursorclass = MySQLdb.cursors.DictCursor)
#cursor = db.cursor()

#number_of_rows = cursor.execute("select * from trans_produced")



#result = {}
#for row in reader:
#     key = row[0]
#     result[key] = row[1:]
# #result = cursor.fetchall()

for i in reader:
    #writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.append(dict(i))
    #producer.send("topic_test", raw_bytes)
    #print(i)

# #     sleep(0.5)

writer.close()
reader = DataFileReader(open("transactions.avro", "rb"), DatumReader(schema, reader_schema))
#new_schema = reader.get_meta("avro.schema")
users = []
for user in reader:
    users.append(user)

#producer.send('topic_test', {1:'hello'})

for i in users:
    print(i)
    producer.send('topic_test', value=i)
    sleep(0.2)
    
# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)
# def on_send_error(excp):
#     logging.error('I am an errback', exc_info=excp)


#print(producer.send('test_topic', b'some_message_bytes').get(timeout=30))


producer.flush()



#producer.close()

reader.close()
#db.close()
