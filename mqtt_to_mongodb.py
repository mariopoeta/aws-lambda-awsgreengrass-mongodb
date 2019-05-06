import paho.mqtt.client as mqtt
import greengrasssdk
import time
import json
import datetime
import os
from pymongo import MongoClient
import __future__

iot_client = greengrasssdk.client('iot-data')

def mqtt_topic():
    mqtt_server_conn = os.environ['mqtt-conn']
    client = mqtt.Client()
    client.on_connect = mqtt_topic
    client.on_message = write_to_mongo
    connOK = False
    while(connOK == False):
        try:
            client.connect(mqtt_server_conn, 1883, 60)
            connOK = True
        except:
            connOK = False
        time.sleep(2)
    # Blocking loop to the Mosquitto broker
    client.loop_forever()
    client.subscribe("opcua/#")

def write_to_mongo(client, userdata, msg):
    mongodb_conn = os.environ['mongodb_connnection']
    mongoClient = MongoClient(mongodb_conn)
    db = mongoClient.plc_poc_db
    collection = db.plc_poc
    receivetime = datetime.datetime.now()
    message = msg.payload.decode("utf-8")
    isvaluefloat = False
    try:
        # Convert the string to a float so that it is stored as a number and not a string in the database
        val = float(message)
        isvaluefloat = True
    except:
        isvaluefloat = False

    if isvaluefloat:
        print(str(receivetime) + ": " + msg.topic + " " + str(val))
        post = {"time": receivetime, "topic": msg.topic, "value": val}
    else:
        print(str(receivetime) + ": " + msg.topic + " " + message)
        post = {"time": receivetime, "topic": msg.topic, "value": message}

    collection.insert_one(post)


def function_handler(event, context):
    return
