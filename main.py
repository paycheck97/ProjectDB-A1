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

# -------------------------------------------------------------
#  -------------------------- QUERYS --------------------------
# -------------------------------------------------------------
def doQueryMeterEnTienda(id_persona, id_tienda):
    cur = conn.cursor()
    sql = "UPDATE estadistica_camara SET dentro_tienda = true WHERE id_persona = '%s';"
    cur.execute(sql, [id_persona])
    sql = "INSERT INTO estadistica_tienda (id_tienda, id_persona, rip) VALUES (%s, %s, %s)"
    cur.execute(sql, (id_tienda, id_persona, False))
    conn.commit()
    print('ENTRA -> persona con cédula ', id_persona, ' a la tienda con id ', id_tienda)

def doQueryCantidadEnPasillo():
    cur = conn.cursor()
    sql = "SELECT * FROM estadistica_camara WHERE dentro_tienda = false AND sentado_mesa = false AND rip = false GROUP BY id;"
    cur.execute(sql)
    records = cur.fetchall()
    return records

def doQueryCantidadEnCadaTienda():
    cur = conn.cursor()
    sql = "SELECT id_tienda, COUNT(id_tienda) FROM estadistica_tienda WHERE rip = false GROUP BY id_tienda HAVING COUNT(id_tienda) > 0 ORDER BY id_tienda;"
    cur.execute(sql)
    records = cur.fetchall()
    return records

def doQueryPersonasEnTienda(id_tienda):
    cur = conn.cursor()
    sql = "SELECT id_persona FROM estadistica_tienda WHERE rip = false AND id_tienda = %s;"
    cur.execute(sql, [id_tienda])
    records = cur.fetchall()
    return records

def doQuerySacarDeTienda(id_persona, id_tienda):
    cur = conn.cursor()
    sql = "UPDATE estadistica_tienda SET rip = true WHERE id_persona = %s AND id_tienda = %s AND rip = false;"
    cur.execute(sql, (id_persona, id_tienda))
    sql = "UPDATE estadistica_camara SET dentro_tienda = false WHERE id_persona = '%s';"
    cur.execute(sql, [id_persona])
    conn.commit()
    print('SALE -> persona con cédula ', id_persona, ' de la tienda con id ', id_tienda)

def doQuerySentarEnMesa(id_persona, id_mesa):
    cur = conn.cursor()
    sql = "UPDATE estadistica_tienda SET rip = true WHERE id_persona = %s AND id_tienda = %s AND rip = false;"
    cur.execute(sql, (id_persona, id_tienda))
    sql = "UPDATE estadistica_camara SET dentro_tienda = false WHERE id_persona = '%s';"
    cur.execute(sql, [id_persona])
    conn.commit()
    print('SALE -> persona con cédula ', id_persona, ' de la tienda con id ', id_tienda)

def doQueryMesasDesocupadas():
    cur = conn.cursor()
    sql = "SELECT id_mesa FROM estadistica_mesa WHERE rip = true;"
    cur.execute(sql)
    records = cur.fetchall()
    return records


# -------------------------------------------------------------
#  ------------------------ FUNCIONES -------------------------
# -------------------------------------------------------------
def meterPersonaEnTienda():
    personas_en_pasillo_query = doQueryCantidadEnPasillo()
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

    # Si hay gente en los pasillos, meter a una persona en la tienda
    if (len(personas_en_pasillo) > 0):
        doQueryMeterEnTienda(id_persona_para_meter, id_tienda_para_meter)

def sacarPersonEnTienda():
    tienda_a_sacar_query = doQueryCantidadEnCadaTienda()
    random.shuffle(tienda_a_sacar_query)
    tienda_a_sacar = tienda_a_sacar_query[0]
    id_tienda_a_sacar = tienda_a_sacar[0]
    cant_tienda_a_sacar = tienda_a_sacar[1]

    personas_en_tienda = doQueryPersonasEnTienda(id_tienda_a_sacar)
    random.shuffle(personas_en_tienda)
    id_persona_a_sacar = personas_en_tienda[0][0]

    doQuerySacarDeTienda(id_persona_a_sacar, id_tienda_a_sacar)

def sentarPersonaEnMesa():
    personas_en_pasillo_query = doQueryCantidadEnPasillo()
    personas_en_pasillo = []
    # Obtenemos las cédulas de las personas que están en los pasillos
    for row in personas_en_pasillo_query:
        personas_en_pasillo.append(row[4])
    cantidad = len(personas_en_pasillo)
    random = int(np.random.uniform(0, cantidad)) # Lugar en el array de la persona que sentaremos en una mesa

    id_mesas_desocupadas_query = doQueryMesasDesocupadas()
    print(id_mesas_desocupadas_query)



    # # Si hay gente en los pasillos, meter a una persona en la tienda
    # if (len(personas_en_pasillo) > 0):
    #     doQueryMeterEnTienda(id_persona_para_meter, id_tienda_para_meter)

def main():
    # meterPersonaEnTienda()

    # sacarPersonEnTienda()

    sentarPersonaEnMesa()

if __name__ == '__main__':
    main()
sys.exit(0)
