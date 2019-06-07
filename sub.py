import ssl
import sys
import json
import paho.mqtt.client
import psycopg2


conn = psycopg2.connect(host='bydl8yhl1xvycflftlju-postgresql.services.clever-cloud.com',
                        user='ujegvu5yu2q28i5dthj9', password='86WWh0iQz4n8dfN82WGW', dbname='bydl8yhl1xvycflftlju')


def doQuery(a):
    a = a.decode('utf-8').replace("'", '"')
    a = json.loads(a)
    
    id_camara = a['id_camara']
    hora_entrada = a['hora_entrada']
    hora_salida = a['hora_salida']
    id_persona = a['id_persona']
    macaddress = a['macaddress']

    cur = conn.cursor()
    sql = "INSERT INTO estadistica_camara (id_camara, hora_entrada, hora_salida, id_persona, dentro_tienda, sentado_mesa, rip) VALUES (%s, %s, %s, %s, %s, %s, %s);"
    cur.execute(sql, (id_camara, hora_entrada, hora_salida, id_persona, False, False, False))
    conn.commit()
    if (macaddress != None):
        print('ENTRA -> persona con cédula ', id_persona, ' al CENTRO COMERCIAL y con el teléfono ', macaddress)
    else:
        print('ENTRA -> persona con cédula ', id_persona, ' al CENTRO COMERCIAL')

def on_connect(client, userdata, flags, rc):
    print("Conectado! (%s)" % client._client_id)
    client.subscribe(topic='sambil/#', qos=0)


def on_message(client, userdata, message):
    doQuery(message.payload)

    

def main():
    client = paho.mqtt.client.Client()
    client.on_connect = on_connect
    client.message_callback_add('sambil/accesos', on_message)
    client.connect(host='localhost')
    client.loop_forever()


if __name__ == '__main__':
    main()

sys.exit(0)
