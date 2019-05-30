import ssl
import sys
import json
import paho.mqtt.client
import psycopg2 


conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='password', dbname= 'testDB')

def doQuery(a):
    a = a.decode('utf-8').replace("'", '"')
    print(a)
    a = json.loads(a)
    var1 = a['id_cam']
    var2 = a['fecha']
    cur = conn.cursor()
    sql = "INSERT INTO stat (id_cam, fecha) VALUES (%s, %s);"
    cur.execute(sql, (var1, var2))
    conn.commit()

def on_connect(client, userdata, flags, rc):
    print("Conectao' (%s)" % client._client_id)
    client.subscribe(topic='local/#', qos = 0) 

def on_message(client, userdata, message): 
    print('topic: %s' % message.topic)
    print('payload: %s' % message.payload)
    print('------------------------------')
    doQuery(message.payload)
    
     

def main():
    client = paho.mqtt.client.Client()
    client.on_connect = on_connect
    client.message_callback_add('local/puerta', on_message)
    client.connect(host='localhost')
    client.loop_forever()

if __name__ == '__main__':
    main()

sys.exit(0)