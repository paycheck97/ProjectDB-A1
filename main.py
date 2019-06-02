import sys
import json
import paho.mqtt.client
import psycopg2
import numpy as np
import math
import random
import datetime


conn = psycopg2.connect(host='bydl8yhl1xvycflftlju-postgresql.services.clever-cloud.com',
                        user='ujegvu5yu2q28i5dthj9', password='86WWh0iQz4n8dfN82WGW', dbname='bydl8yhl1xvycflftlju')

def doQueryTienda():
    cur = conn.cursor()
    sql = "Select * FROM estadistica_camara WHERE dentro_tienda = false AND rip = false GROUP BY id;"
    cur.execute(sql)
    records = cur.fetchall()
    return records

def main():
    personas_en_pasillo_query = doQueryTienda()
    personas_en_pasillo = []
    # Obtenemos las cédulas de las personas que están en los pasillos
    for row in personas_en_pasillo_query:
        personas_en_pasillo.append(row[4])
    cantidad = len(personas_en_pasillo)
    random = int(np.random.uniform(0, cantidad)) # Lugar en el array de la persona que meteremos en una tienda
    id_tienda_para_meter = int(np.random.uniform(1, 4)) # ID de la tienda en lña que vamos a meter
    cont = 0

    for row in personas_en_pasillo:
        if (cont == random):
            id_persona_para_meter = row
        cont+=1

    print(id_persona_para_meter)
    

    # print(personas_en_pasillo)
    # print(cantidad)
        

if __name__ == '__main__':
    main()
sys.exit(0)




