# create a cursor
import psycopg2
#connect to database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="example")

cur = conn.cursor()

# execute a statement
cur.execute('SELECT version()')

# display the PostgreSQL database server version
db_version = cur.fetchone()

print('PostgreSQL database version:')
print(db_version)

# close the communication with the PostgreSQL
cur.close()