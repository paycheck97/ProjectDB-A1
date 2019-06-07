import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import datetime
import math
import psycopg2
from randmac import RandMac


def on_connect(client, userdata, flags, rc):
    print('Publicando')


def main():
    conn = psycopg2.connect(host='bydl8yhl1xvycflftlju-postgresql.services.clever-cloud.com', user='ujegvu5yu2q28i5dthj9', password='86WWh0iQz4n8dfN82WGW', dbname='bydl8yhl1xvycflftlju')
    cursor = conn.cursor()
    conn.autocommit = True
  
    client = paho.mqtt.client.Client("Pipo-subs", False)
    client.qos = 0
    client.connect(host='localhost')
    
    while(True):
        random = int(np.random.uniform(0, 2))
        if (random == 1):
            sexo = 'masculino'
        else:
            sexo = 'femenino'

        random = int(np.random.uniform(0, 2))
        tiene_telefono = False
        macaddress = None
        if (random == 1):
            tiene_telefono = True
            macaddress = str(RandMac("00:00:00:00:00:00", False))
            macaddress = macaddress.replace("'", "")
            
        edad = int(np.random.uniform(10, 60))
        hora_entrada = time.strftime("%d/%m/%y %H:%M:%S")
        id_camara = int(np.random.uniform(1, 4))
        id_persona = int(np.random.uniform(1000000, 50000000))
        
        payload = {
            "id_camara": id_camara,
            "hora_entrada": hora_entrada,
            "hora_salida": None,
            "id_persona": id_persona,
            "macaddress": macaddress
        }
        
        # Crear Persona en la BDD
        query = "INSERT INTO persona (id, sexo, edad) VALUES (%s, %s, %s);"
        cursor.execute(query, (id_persona, sexo, edad))
        conn.commit()

        # Crear Registro de teléfono en caso de que tenga teléfono
        if (tiene_telefono == True):
            query = "INSERT INTO telefono (macaddress, id_persona) VALUES (%s, %s);"
            cursor.execute(query, (macaddress, id_persona))
            conn.commit()
        
        client.publish('sambil/accesos', json.dumps(payload), qos=0)

        time.sleep(np.random.uniform(2, 4))


if __name__ == '__main__':
    main()

sys.exit(0)