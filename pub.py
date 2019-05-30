import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import datetime


def on_connect(client, userdata, flags, rc):
    print('Publicando')

def main():
    client = paho.mqtt.client.Client("Pipo-subs", False)
    client.qos = 0
    client.connect(host='localhost')
    info =100
    horaAct= datetime.datetime.now().replace(minute=0, second=0)
    while(info>0):
        hora = horaAct + datetime.timedelta(minutes=np.random.uniform(0,60))
        cam = int(np.random.uniform(1,3))
        payload = {
            "id_cam": str(cam),
            "fecha": str(hora)
        }
        client.publish('local/puerta',json.dumps(payload),qos=0)
        print(payload)
        time.sleep(1)
        info = info -1
        
if __name__ == '__main__':
    main()
    
sys.exit(0)
