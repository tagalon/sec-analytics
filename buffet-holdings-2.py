import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost", database="postgres", user="postgres", password="example"
)

cur = conn.cursor()

cur.execute("""
SELECT tmp_table.total_shares, 
        holding_infos.cusip, 
        holding_infos.ticker, 
        holding_infos.security_name, 
        filings.period_of_report, 
        filings.filing_id
FROM
(
  SELECT SUM(shares) as total_shares, cusip, filing_id
  FROM holdings
  GROUP BY cusip, filing_id
) tmp_table
INNER JOIN filings
ON filings.filing_id = tmp_table.filing_id
INNER JOIN holding_infos
ON tmp_table.cusip = holding_infos.cusip
WHERE (filings.period_of_report = '2021-06-30' or filings.period_of_report = '2021-03-31') and filings.cik = '1067983'
""")

rows = cur.fetchall()    

cur.close()

buffet_holdings = pd.DataFrame(rows, columns =['Shares', 
                                               'CUSIP', 
                                               'Ticker',                                                
                                               'SecurityName', 
                                               'PeriodOfReport', 
                                               'FilingId'])
print(buffet_holdings)