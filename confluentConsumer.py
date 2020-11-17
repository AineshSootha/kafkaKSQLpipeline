from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import os
import io
import json
import logging
import MySQLdb as sql
import MySQLdb.cursors
import avro as av
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
#import MySQLdb as sql
#import MySQLdb.cursors
#import kafka
#from time import sleep
#from kafka import KafkaProducer
#from kafka import KafkaClient
# from confluent_kafka import Producer
# from confluent_kafka.avro import AvroProducer
#from kafka.errors import KafkaError
#import avro.schema
#from avro.datafile import DataFileReader, DataFileWriter
#from avro.io import DatumReader, DatumWriter
import csv
#from schema_registry.client import SchemaRegistryClient, schema

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

def dictToTransaction(trans, ctx):
    if trans is None:
        return None
    
    return Transaction(BASKET_NUM=trans["BASKET_NUM"], HSHD_NUM=trans["HSHD_NUM"], PURCHASE_=trans["PURCHASE_"],
     PRODUCT_NUM=trans["PRODUCT_NUM"], SPEND=trans["SPEND"], UNITS=trans["UNITS"], STORE_R=trans["STORE_R"],
     WEEK_NUM=trans["WEEK_NUM"], YEAR=trans["YEAR"], id=trans["id"] )


def MySQLConsumer(hostname, portnum, uname, paswd, dbase, tablename, comsumeList):
    #for i in consumeList:
    db = sql.connect(host =hostname, port = portnum, user = uname, passwd = paswd, db = dbase, cursorclass = MySQLdb.cursors.DictCursor)
    cursor = db.cursor()
    cursor.executemany("""INSERT INTO transactions_consumed(BASKET_NUM, HSHD_NUM, PURCHASE_, PRODUCT_NUM, SPEND, UNITS, STORE_R, WEEK_NUM, YEAR, id) VALUES (%(BASKET_NUM)s, %(HSHD_NUM)s, %(PURCHASE_)s, %(PRODUCT_NUM)s, %(SPEND)s, %(UNITS)s, %(STORE_R)s, %(WEEK_NUM)s, %(YEAR)s, %(id)s);""", consumeList)
    db.commit()

def MySQLAlterByID(hostname, portnum, uname, paswd, dbase, tablename):
    db = sql.connect(host =hostname, port = portnum, user = uname, passwd = paswd, db = dbase, cursorclass = MySQLdb.cursors.DictCursor)
    cursor = db.cursor()
    cursor.execute("ALTER TABLE " +  tablename + " ORDER BY id ASC;")
    db.commit()

# c = AvroConsumer({
#     'bootstrap.servers': 'kafka:29092',
#     'group.id': 'test1',
#     'schema.registry.url': 'http://schema-registry:8081'
#     })


#c.subscribe(['topic_test'])

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
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(value_schema_str, schema_registry_client, dictToTransaction)
string_deserializer = StringDeserializer('utf_8')


consumer_conf = {'bootstrap.servers': 'kafka:29092',
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': "group1"}
                     #'auto.offset.reset': "earliest"}
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['topic_test'])
consumeList = []
while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        trans = msg.value()
        if trans is not None:
            consumeList.append(dict(BASKET_NUM=trans.BASKET_NUM, HSHD_NUM = trans.HSHD_NUM, PURCHASE_=trans.PURCHASE_,
     PRODUCT_NUM=trans.PRODUCT_NUM, SPEND=trans.SPEND, UNITS=trans.UNITS, STORE_R=trans.STORE_R,
     WEEK_NUM=trans.WEEK_NUM, YEAR=trans.YEAR, id=trans.id))
        if len(consumeList) >= 1000:
            MySQLConsumer("mysql_db", 3306, "root", "password", "pythonTest", "transactions_consumed", consumeList) 
            print(consumeList[-1])
            consumeList = []   
        
    except KeyboardInterrupt:
        if consumeList:
            MySQLConsumer("mysql_db", 3306, "root", "password", "pythonTest", "transactions_consumed", consumeList) 
            MySQLAlterByID("mysql_db", 3306, "root", "password", "pythonTest", "transactions_consumed")
            consumeList = []   
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())
consumer.close()