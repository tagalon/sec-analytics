import psycopg2

conn = psycopg2.connect(
    host="localhost", database="postgres", user="postgres", password="example"
)

cur = conn.cursor()

cur.execute("""
SELECT COUNT(DISTINCT cik) as number_of_funds
FROM "filings"
WHERE period_of_report='2021-06-30'
""")

result = cur.fetchone()    

cur.close()

print(f"Total number of funds filed for period 2021-06-30:\n{result[0]}")