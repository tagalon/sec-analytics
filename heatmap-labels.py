import psycopg2
import pandas as pd
import math

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

total_number_of_funds = 5932
top_holdings['CounterRelative'] = [round(counter/total_number_of_funds * 100,0) for counter in top_holdings['Counter']]


# heatmap shape: 10 rows, 10 columns
shape = (10, 10) 

tickers = np.asarray(top_holdings['Ticker']).reshape(shape)
counters = np.asarray(top_holdings['CounterRelative']).reshape(shape)

top_holdings['Position'] = range(1,len(top_holdings) + 1)
top_holdings['y_rows'] = [(math.floor(x/shape[1]) + 1 if x%shape[1] != 0 else math.floor(x/shape[1])) for x in top_holdings['Position']]
top_holdings['x_cols'] = [(x%shape[1] if x%shape[1] != 0 else shape[1]) for x in top_holdings['Position']]

pivot_table = top_holdings.pivot(index='y_rows', columns='x_cols', values='CounterRelative')

heatmap_labels = np.asarray(["{0} \n {1}%".format(ticker, counter) 
                              for ticker, counter in zip(tickers.flatten(), counters.flatten())]).reshape(shape)
