import psycopg2
#connect to database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="example")