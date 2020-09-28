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

# reader_schema = avro.schema.parse(json.dumps({
#     "namespace": "customers.avro",
#     "type": "record",
#     "name": "pythonTest",
#     "fields": [
#         {"name" : "id", "type": "string"},
#         {"name" : "first_name", "type": "string"},
#         {"name" : "last_name", "type": "string"},
#         {"name" : "email", "type": "string"},
#         {"name" : "gender", "type": "string"},
#         {"name" : "club_status", "type": "string"},
#         {"name" : "comments", "type": "string"}
#     ]
# }))

writer = DataFileWriter(open("transactions.avro", "wb"), DatumWriter(), schema)

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
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.append(dict(i))



writer.close()

reader = DataFileReader(open("transactions.avro", "rb"), DatumReader(schema, reader_schema))

users = []
for user in reader:
    users.append(user)


for i in users:
    # print(i)
    producer.send('topic_test', value=i)
    sleep(0.2)



producer.flush()

reader.close()
#db.close()
