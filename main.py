import json
import random
import os
import datetime

from s3 import S3Client

import paho.mqtt.client as mqtt

s3 = S3Client(
    access_key=os.environ.get('access'),
    secret_key=os.environ.get('secret'),
    region='ru-central1',
    s3_bucket='test-storage-iot'
)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(cl, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("razzerone")


# The callback for when a PUBLISH message is received from the server.
def on_message(cl, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

    data = json.load(msg.payload)
    data['time'] = str(datetime.datetime.utcnow())

    data_json = json.dumps(data)

    #s3.upload(f'{datetime.datetime.now()}-message.json', bytes(data_json),
    #          content_type="application/json")


client_id = f'python-mqtt-{random.randint(0, 1000)}'
client = mqtt.Client(client_id)
client.username_pw_set('admin', 'admin')
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.1.102", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
