#!/usr/bin/env python2

import paho.mqtt.client as mqtt
import greengrasssdk
import time
import json
import datetime
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, InvalidName
import __future__

def on_connect(mqttc, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    mqttc.subscribe("/opcua/#")

def on_message(mqttc, userdata, msg, collection):
    receiveTime = datetime.datetime.now()
    message = msg.payload.decode("utf-8")
    isfloatValue = False
    try:
        # Convert the string to a float so that it is stored as a number and not a string in the database
        val = float(message)
        isfloatValue = True
    except:
        isfloatValue = False

    if isfloatValue:
        print(str(receiveTime) + ": " + msg.topic + " " + str(val))
        post = {"time": receiveTime, "topic": msg.topic, "value": val}
    else:
        print(str(receiveTime) + ": " + msg.topic + " " + message)
        post = {"time": receiveTime, "topic": msg.topic, "value": message}

    collection.insert_one(post)

def mongodb_connection():
    mongodb_conn = os.environ['mongodb_connnection']
    mongoClient = MongoClient(mongodb_conn)
    try:
        mongoClient.admin.command('ismaster')
    except ConnectionFailure:
        print("Server not available")
    db = mongoClient.plc_poc_db
    collection = db.plc_poc
    return collection

def topic_to_mongo():
    mqtt_server_conn = os.environ['mqtt_conn']
    mqttc = mqtt.Client(mqtt_server_conn)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.loop_forever()

def function_handler(event, context):
    mongodb_connection()
    topic_to_mongo()
