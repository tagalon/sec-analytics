import psycopg2
#connect to database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="example")

cur = conn.cursor()
cur.execute("SELECT count(*) as COUNTER FROM holdings")
counter = cur.fetchone()
print(counter[0])
cur.close()