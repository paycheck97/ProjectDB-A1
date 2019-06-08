import matplotlib
import matplotlib.pyplot as plt
import psycopg2
import pandas as pd

conn = psycopg2.connect(host='bydl8yhl1xvycflftlju-postgresql.services.clever-cloud.com',
                        user='ujegvu5yu2q28i5dthj9', password='86WWh0iQz4n8dfN82WGW', dbname='bydl8yhl1xvycflftlju')

cur = conn.cursor()
sql = "SELECT COUNT(id) FROM venta WHERE id_persona IS NOT null;"
cur.execute(sql)
conTel = cur.fetchall()
conTel = conTel[0][0]

cur = conn.cursor()
sql = "SELECT COUNT(id) FROM venta WHERE id_persona IS null;"
cur.execute(sql)
sinTel = cur.fetchall()
sinTel = sinTel[0][0]

total = conTel + sinTel


names = ['Grupo con Telefono', 'Grupo sin Telefono']
values = [(conTel/2)*100, (sinTel/2)*100]

plt.figure(figsize=(15, 5))

plt.subplot(131)
plt.bar(names, values)
plt.grid(True)
plt.ylabel('Valores Porcentuales')
plt.suptitle('Ventas (porcentaje)')
plt.axis([-0.5, 1.5, 0 , 101])
plt.show  #grafico comparativa de compras con y sin telefono

#Tabla en una celda
sql = '''
    SELECT * FROM numero_accesos
'''

df = pd.read_sql(sql, conn)

#En otra celda
df
sql = '''
    SELECT * FROM mas_visitas;
'''

df = pd.read_sql(sql, conn)
df