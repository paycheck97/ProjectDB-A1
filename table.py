import psycopg2 
import pandas as pd

conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='password', dbname= 'testDB')

sql = '''
    SELECT stat.fecha, camara.id, acceso.nombre_acceso, centro_comercial.nombre
    FROM stat
    INNER JOIN camara ON stat.id_cam = camara.id
    INNER JOIN acceso ON camara.id_acc = acceso.id
    INNER JOIN centro_comercial 
    ON acceso.id_cc = centro_comercial.id;
'''

df = pd.read_sql(sql, conn)
df