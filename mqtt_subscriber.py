import greengrasssdk
import time
import json
import datetime
import os
import __future__



iot_client = greengrasssdk.client('iot-data')

def subscribe_topic():
    iot_client.subscribe("opcua/#")

def write_to_mongo(client, userdata, msg):
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

mongodb_conn = os.environ['mongodb_connnection']

mongoClient = MongoClient(mongodb_conn)
db = mongoClient.plc_poc_db
collection = db.plc_poc

def function_handler(event, context):
    return
