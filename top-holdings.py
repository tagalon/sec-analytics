import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="example")

cur = conn.cursor()

cur.execute("""
SELECT COUNT(tmp_table.cusip) as counter, tmp_table.cusip, tmp_table.ticker, tmp_table.security_name
FROM (
    SELECT DISTINCT holdings.cusip, ticker, security_name, holdings.filing_id
    FROM "holdings"
    INNER JOIN "filings"
    ON filings.filing_id = holdings.filing_id
    INNER JOIN holding_infos
    ON holdings.cusip = holding_infos.cusip
    WHERE period_of_report = '2021-06-30' and not holdings.cusip LIKE '000%'
) tmp_table
GROUP BY tmp_table.cusip, tmp_table.ticker, tmp_table.security_name
ORDER BY counter DESC
LIMIT 100
""")

top_holdings = cur.fetchall()    
top_holdings = pd.DataFrame(top_holdings, columns =['Counter ','CUSIP', 'Ticker', 'SecurityName'])

cur.close()

print(top_holdings)