import sys
import json
import paho.mqtt.client
import psycopg2
import numpy as np
import math
import random
import datetime
import time

conn = psycopg2.connect(host='bydl8yhl1xvycflftlju-postgresql.services.clever-cloud.com',
                        user='ujegvu5yu2q28i5dthj9', password='86WWh0iQz4n8dfN82WGW', dbname='bydl8yhl1xvycflftlju')

# -------------------------------------------------------------
#  -------------------------- QUERYS --------------------------
# -------------------------------------------------------------
def doQueryPersonasEnPasillo():
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

def doQueryMesasOcupadas():
    cur = conn.cursor()
    sql = "SELECT id_mesa FROM estadistica_mesa WHERE rip = false ORDER BY id_mesa;"
    cur.execute(sql)
    records = cur.fetchall()
    return records

def doQueryMeterEnTienda(id_persona, id_tienda):
    cur = conn.cursor()
    sql = "UPDATE estadistica_camara SET dentro_tienda = true WHERE id_persona = '%s';"
    cur.execute(sql, [id_persona])
    sql = "INSERT INTO estadistica_tienda (id_tienda, id_persona, rip) VALUES (%s, %s, %s)"
    cur.execute(sql, (id_tienda, id_persona, False))
    conn.commit()
    print('ENTRA -> persona con cédula ', id_persona, ' a la tienda con id ', id_tienda)

def doQuerySacarDeTienda(id_persona, id_tienda):
    cur = conn.cursor()
    sql = "UPDATE estadistica_tienda SET rip = true WHERE id_persona = %s AND id_tienda = %s AND rip = false;"
    cur.execute(sql, (id_persona, id_tienda))
    sql = "UPDATE estadistica_camara SET dentro_tienda = false WHERE id_persona = '%s';"
    cur.execute(sql, [id_persona])
    conn.commit()
    print('SALE -> persona con cédula ', id_persona, ' de la tienda con id ', id_tienda)

def doQuerySentarEnMesa(id_persona, id_mesa, hora_entrada):
    cur = conn.cursor()
    sql = "UPDATE estadistica_mesa SET rip = false, hora_entrada = %s, id_persona = %s, hora_salida = null WHERE id_mesa = %s;"
    cur.execute(sql, (hora_entrada, id_persona, id_mesa))
    if(id_persona != None):
        sql = "UPDATE estadistica_camara SET sentado_mesa = true WHERE id_persona = '%s';"
        cur.execute(sql, [id_persona])
    conn.commit()
    print('SE Sento -> persona con cédula ', id_persona, ' en la mesa con id ', id_mesa)

def doQueryTieneTelefono(id_persona):
    cur = conn.cursor()
    sql = "SELECT * FROM telefono WHERE id_persona = %s;"
    cur.execute(sql, [id_persona])
    conn.commit()
    records = cur.fetchall()
    return records

def doQueryPersonasEnMesa(id_mesa):
    cur = conn.cursor()
    sql = "SELECT id_persona FROM estadistica_mesa WHERE id_mesa = %s;"
    cur.execute(sql, [id_mesa])
    records = cur.fetchall()
    return records

def doQueryPararDeMesa(id_mesa, id_persona, hora_salida):
    cur = conn.cursor()
    sql = "UPDATE estadistica_mesa SET hora_salida = %s, rip = true WHERE id_mesa = %s;"
    cur.execute(sql, (hora_salida, id_mesa))
    if(id_persona != None):
        sql = "UPDATE estadistica_camara SET sentado_mesa = false WHERE id_persona = '%s';"
        cur.execute(sql, [id_persona])
    conn.commit()
    print('SE Paro -> persona con cédula ', id_persona, ' de la mesa con id ', id_mesa)

def doQueryRealizarVenta(id_tienda, id_persona, monto):
    cur = conn.cursor()
    sql = "INSERT INTO venta (id_tienda, id_persona, monto) VALUES (%s, %s, %s)"
    cur.execute(sql, (id_tienda, id_persona, monto))
    conn.commit()
    print('COMPRA -> persona con cédula ', id_persona, ' a la tienda con id ', id_tienda, ' por un monto ', monto)

def doQuerySacarDelCC(id_persona):
    cur = conn.cursor()
    sql = "UPDATE estadistica_camara SET rip = true WHERE id_persona = '%s' and rip = false;"
    cur.execute(sql, [id_persona])
    conn.commit()
    print('SALE -> persona con cédula ', id_persona, ' del centro comercial')


# -------------------------------------------------------------
#  ------------------------ FUNCIONES -------------------------
# -------------------------------------------------------------
def meterPersonaEnTienda():
    personas_en_pasillo_query = doQueryPersonasEnPasillo()
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
    #cant_tienda_a_sacar = tienda_a_sacar[1]

    personas_en_tienda = doQueryPersonasEnTienda(id_tienda_a_sacar)
    random.shuffle(personas_en_tienda)
    id_persona_a_sacar = personas_en_tienda[0][0]

    doQuerySacarDeTienda(id_persona_a_sacar, id_tienda_a_sacar)

def sentarPersonaEnMesa():
    personas_en_pasillo_query = doQueryPersonasEnPasillo()
    personas_en_pasillo = []

    # Obtenemos las cédulas de las personas que están en los pasillos
    for row in personas_en_pasillo_query:
        personas_en_pasillo.append(row[4])

    cantidad = len(personas_en_pasillo)
    randomPersona = int(np.random.uniform(0, cantidad)) # Lugar en el array de la persona que sentaremos en una mesa

    cont = 0
    for row in personas_en_pasillo:
        if (cont == randomPersona):
            id_persona_para_sentar = row
        cont+=1

    id_mesas_ocupadas_query = doQueryMesasOcupadas()
    id_mesas_ocupadas = []
    id_mesas_desocupadas = []
    for id in id_mesas_ocupadas_query:
        id_mesas_ocupadas.append(id[0])

    for i in range(0, 9):
        if (not find(id_mesas_ocupadas, i + 1)):
            id_mesas_desocupadas.append(i + 1)  

    hora_entrada = time.strftime("%d/%m/%y %H:%M:%S")        
            
    random.shuffle(id_mesas_desocupadas)
    id_mesa_para_sentar = id_mesas_desocupadas[0]

    verificarTelefono(id_persona_para_sentar) #chequea si el id de la persona corresponde a una con telefono

    if(not verificarTelefono): #si no tiene telefono no guarda el id en la mesa
        id_persona_para_sentar = None


    doQuerySentarEnMesa(id_persona_para_sentar, id_mesa_para_sentar , hora_entrada)

def pararPersonaDeMesa():
    mesas_ocupadas = doQueryMesasOcupadas() #se elige una mesa que este ocupada
    random.shuffle(mesas_ocupadas)
    mesa_a_desocupar = mesas_ocupadas[0][0]

    persona_a_parar = doQueryPersonasEnMesa(mesa_a_desocupar)
    hora_salida = time.strftime("%d/%m/%y %H:%M:%S")
    persona_a_parar = persona_a_parar[0][0]
    doQueryPararDeMesa(mesa_a_desocupar, persona_a_parar, hora_salida)

def realizarVenta():
    tienda_a_vender_query = doQueryCantidadEnCadaTienda() #selecciona que tienda va a realizar una venta
    random.shuffle(tienda_a_vender_query)
    id_tienda_a_vender = tienda_a_vender_query[0][0]

    personas_en_tienda = doQueryPersonasEnTienda(id_tienda_a_vender) #selecciona que persona dentro de la tienda va a comprar
    random.shuffle(personas_en_tienda)
    id_persona_a_comprar = personas_en_tienda[0][0]

    verificarTelefono(id_persona_a_comprar)
    if(verificarTelefono != True): #si no tiene telefono no adjudica la venta a un id
        id_persona_a_comprar = None

    monto = np.random.uniform(100, 50000000)
    monto = round(monto, 2)

    doQueryRealizarVenta(id_tienda_a_vender,id_persona_a_comprar, monto)

def sacarPersonaEnCC():
    personas_en_pasillo_query = doQueryPersonasEnPasillo()
    random.shuffle(personas_en_pasillo_query)
    if (len(personas_en_pasillo_query) > 0):
        persona_a_sacar_cc = personas_en_pasillo_query[0][4]
        doQuerySacarDelCC(persona_a_sacar_cc)

def find(array, value):
    for val in array:
        if (val == value):
            return True
    return False

def verificarTelefono(id_persona):
    chequeo = doQueryTieneTelefono(id_persona)
    print(chequeo)
    if(len(chequeo) > 0):
        return True
    return False

def main():
    # meterPersonaEnTienda()

    # sacarPersonEnTienda()

    #sentarPersonaEnMesa()

    #pararPersonaDeMesa()

    realizarVenta()

    #sacarPersonaEnCC()

if __name__ == '__main__':
    main()
sys.exit(0)
