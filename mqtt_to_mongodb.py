import paho.mqtt.client as mqtt
import greengrasssdk
import time
import json
import datetime
import os
from pymongo import MongoClient
import __future__


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("opcua/#")


def on_message(client, userdata, msg):
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


# Set up client for MongoDB
mongodb_conn = os.environ['mongodb_connnection']
mongoClient = MongoClient(mongodb_conn)
db = mongoClient.plc_poc_db
collection = db.plc_poc

# Initialize the client that should connect to the Mosquitto broker
mqtt_server_conn = os.environ['mqtt_conn']
client = mqtt.Client(mqtt_server_conn)
client.on_connect = on_connect
client.on_message = on_message
connOK = False
while(connOK == False):
    try:
        client.connect("mqtt_server_conn", 1883, 60)
        connOK = True
    except:
        connOK = False
    time.sleep(2)

# Blocking loop to the Mosquitto broker
client.loop_forever()

def function_handler(event, context):
    return
