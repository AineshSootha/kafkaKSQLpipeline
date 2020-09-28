import os
import json
import MySQLdb as sql
import MySQLdb.cursors
#import kafka
from time import sleep
from kafka import KafkaProducer
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv


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

writer = DataFileWriter(open("transactions.avro", "wb"), DatumWriter(), schema)

#writer.append({"id": "1000", "first_name": "Ainesh", "last_name": "Sootha", "email": "ainesh", "gender": "M", "club_status": "as","comments": "nao"})

producer = KafkaProducer(
    bootstrap_servers='172.24.0.3:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

db = sql.connect(host ='172.24.0.2', port = 3306, user ='root', passwd = 'password', db ='newPython', cursorclass = MySQLdb.cursors.DictCursor)
cursor = db.cursor()

number_of_rows = cursor.execute("select * from trans_produced")

reader = csv.DictReader(open('../customers.csv'))

#result = {}
#for row in reader:
#     key = row[0]
#     result[key] = row[1:]
# #result = cursor.fetchall()

for i in reader:
    writer.append(dict(i))
#     print(i)
    producer.send('topic_test', value=jd)
#     sleep(0.5)

writer.close()
reader = DataFileReader(open("transactions.avro", "rb"), DatumReader(schema, reader_schema))
new_schema = reader.get_meta("avro.schema")
users = []
for user in reader:
    users.append(user)

for i in users:
    print(i)
reader.close()
# #db.close()
