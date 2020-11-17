from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import os
import io
import json
import logging
import avro as av
import MySQLdb as sql
import MySQLdb.cursors
import csv
from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

class Transaction(object):
    def __init__(self, BASKET_NUM, HSHD_NUM, PURCHASE_, PRODUCT_NUM, SPEND, UNITS, STORE_R, WEEK_NUM, YEAR, id):
        self.BASKET_NUM = BASKET_NUM
        self.HSHD_NUM = HSHD_NUM
        self.PURCHASE_ = PURCHASE_
        self.PRODUCT_NUM = PRODUCT_NUM
        self.SPEND = SPEND
        self.UNITS = UNITS
        self.STORE_R = STORE_R
        self.WEEK_NUM = WEEK_NUM
        self.YEAR = YEAR
        self.id = id

def transactionToDict(trans, ctx):
    return dict(BASKET_NUM=trans["BASKET_NUM"], HSHD_NUM=trans["HSHD_NUM"], PURCHASE_=trans["PURCHASE_"],
     PRODUCT_NUM=trans["PRODUCT_NUM"], SPEND=trans["SPEND"], UNITS=trans["UNITS"], STORE_R=trans["STORE_R"],
     WEEK_NUM=trans["WEEK_NUM"], YEAR=trans["YEAR"], id=trans["id"] )


def MySQLRead(hostname, portnum, uname, paswd, dbase, tablename, id):
    db = sql.connect(host =hostname, port = portnum, user = uname, passwd = paswd, db = dbase, cursorclass = MySQLdb.cursors.DictCursor)
    cursor = db.cursor()
    cursor.execute("SELECT * FROM " + tablename + " WHERE id = " + str(id) + ";")
    return cursor.fetchall()


def MySQLCount(hostname, portnum, uname, paswd, dbase, tablename):
    db = sql.connect(host =hostname, port = portnum, user = uname, passwd = paswd, db = dbase, cursorclass = MySQLdb.cursors.DictCursor)
    cursor = db.cursor()
    cursor.execute("SELECT count(*) FROM " + tablename + ";")
    return cursor.fetchone()

def MySQLProducer(hostname, portnum, uname, paswd, dbase, tablename, schema):
    countRows = MySQLCount(hostname, portnum, uname, paswd, dbase, tablename)["count(*)"]

    j = 1
    for i in range(1,countRows+1):
        key = {"id": j}
        rawdata = MySQLRead(hostname, portnum, uname, paswd, dbase, tablename, i)
        rawdata = rawdata[0]
        print(type(rawdata), rawdata)
        avroProducer.produce(topic='topic_test', value=rawdata, key=key)
        j+=1
    
    avroProducer.flush()


def CSVRead(csvfilename):
    reader =  csv.DictReader(open(csvfilename))

    return reader


def AvroSerialize(schema, msg):
    writer = av.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = av.io.BinaryEncoder(bytes_writer)
    writer.write(dict(msg), encoder)
    raw_data = bytes_writer.getvalue()
    print(type(raw_data))
    return raw_data

def CSVProducer(filename, schema):
    reader = CSVRead(filename)
    j = 1
    for i in reader:
        #print(i)
        #raw_data = AvroSerialize(schema, i)
        key = {"id": j}
        avroProducer.produce(topic='topic_test', value=i, key=key)
        j+=1
    
    avroProducer.flush()

value_schema_str = """
{
  "name": "pythonTest",
  "type": "record",
  "namespace": "transactions.avro",
  "fields": [
    {
      "name": "BASKET_NUM",
      "type": ["int", "null"]
    },
    {
      "name": "HSHD_NUM",
      "type": ["int", "null"]
    },
    {
      "name": "PURCHASE_",
      "type": ["string", "null"]
    },
    {
      "name": "PRODUCT_NUM",
      "type": ["int", "null"]
    },
    {
      "name": "SPEND",
      "type": ["float", "null"]
    },
    {
      "name": "UNITS",
      "type": ["int", "null"]
    },
    {
      "name": "STORE_R",
      "type": ["string", "null"]
    },
    {
      "name": "WEEK_NUM",
      "type": ["int", "null"]
    },
    {
      "name": "YEAR",
      "type": ["int", "null"]
    },
    {
      "name": "id",
      "type": ["int", "null"]
    }
  ]
}
"""
# value_schema_str = """
# {
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
# }
# """

key_schema_str = """
{
    "namespace": "customers.avro",
    "type": "record",
    "name": "key",
    "fields": [
        {"name" : "id", "type": "int"}
    ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)



def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


hostname = "mysql_db"
portnum = 3306
uname = "root"
paswd = "password"
dbase = "pythonTest"
tablename = "transactions_produced"

schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(value_schema_str, schema_registry_client, transactionToDict)
producer_conf = {'bootstrap.servers': 'kafka:29092',
                    'key.serializer': StringSerializer('utf_8'),
                    'value.serializer': avro_serializer}

producer = SerializingProducer(producer_conf)
while True:
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try: 
        countRows = MySQLCount(hostname, portnum, uname, paswd, dbase, tablename)["count(*)"]
        j = 1
        for i in range(1,countRows+1):
            key = {"id": j}
            rawdata = MySQLRead(hostname, portnum, uname, paswd, dbase, tablename, i)
            print(j)
            rawdata = rawdata[0]
            producer.produce(topic='topic_test', key=str(uuid4()), value=rawdata,on_delivery=delivery_report)
            j+=1
            if(j == 99999):
                producer.flush()
           
        break
    except KeyboardInterrupt:
        break

print("\nFlushing records...")
producer.flush()

# avroProducer = AvroProducer({
#     'bootstrap.servers': 'kafka:29092',
#     'on_delivery': delivery_report,
#     'schema.registry.url': 'http://schema-registry:8081'
#     }, default_key_schema=key_schema, default_value_schema=value_schema)

#CSVProducer('../customers.csv', value_schema)

#MySQLProducer("mysql_db", 3306, "root", "password", "pythonTest", "transactions_produced", value_schema)