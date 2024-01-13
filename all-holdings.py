import psycopg2
import pandas as pd


conn = psycopg2.connect(
    host="localhost", database="postgres", user="postgres", password="example"
)

cur = conn.cursor()

cur.execute("""
SELECT SUM(tmp_table.shares), tmp_table.cusip, ticker, security_name, tmp_table.period_of_report
FROM
( 
  SELECT shares, cusip, filings.period_of_report
  FROM holdings
  INNER JOIN filings
  ON filings.filing_id = holdings.filing_id
  WHERE (filings.period_of_report = '2021-06-30' or filings.period_of_report = '2021-03-31') and not cusip LIKE '000%'
) tmp_table
INNER JOIN holding_infos
ON tmp_table.cusip = holding_infos.cusip
WHERE LENGTH(ticker) <= 5 and LENGTH(ticker) >= 1 and not (LENGTH(ticker)= 5 and ticker LIKE '%W')
GROUP BY tmp_table.cusip, ticker, security_name, tmp_table.period_of_report
""")
 

rows = cur.fetchall()    

cur.close()

all_holdings = pd.DataFrame(rows, columns =['Shares', 'CUSIP', 'Ticker', 'SecurityName', 'PeriodOfReport'])

print(all_holdings)