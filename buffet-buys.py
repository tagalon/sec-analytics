
from datetime import datetime
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


def calculate_holding_changes(df):
    date_2021_06_30 = datetime.strptime('2021-06-30', '%Y-%m-%d').date()
    date_2021_03_31 = datetime.strptime('2021-03-31', '%Y-%m-%d').date()

    holdings_changes = "NaN"
    holding_changes = []
    
    subset = df[df['PeriodOfReport']==date_2021_06_30]
    
    # mask_1 = select holdings for 2021-03-31 period
    mask_1 = df['PeriodOfReport']==date_2021_03_31

    for index, holding_2021_06_30 in subset.iterrows():
        # mask_2 = select holdings with same NameOfIssuer
        mask_2 = df['CUSIP']==holding_2021_06_30['CUSIP']
        # merge both masks
        holdings_2021_03_31 = df[(mask_1 & mask_2)]

        if len(holdings_2021_03_31) != 0:
            holding_2021_03_31 = holdings_2021_03_31.iloc[0]

            share_delta_absolute = holding_2021_06_30['Shares'] - holding_2021_03_31['Shares']
            share_delta_relative = (share_delta_absolute / holding_2021_03_31['Shares']) * 100
            shares_2021_06_30 = holding_2021_06_30['Shares']
            shares_2021_03_31 = holding_2021_03_31['Shares']

        else:
            # holding didn't exist in 2021-03-31 filing    
            share_delta_absolute = holding_2021_06_30['Shares']
            share_delta_relative = 100
            shares_2021_06_30 = holding_2021_06_30['Shares']
            shares_2021_03_31 = 0

        holding_changes.append((holding_2021_06_30['CUSIP'], 
                                holding_2021_06_30['Ticker'], 
                                holding_2021_06_30['SecurityName'], 
                                shares_2021_03_31,
                                shares_2021_06_30,
                                share_delta_absolute, 
                                share_delta_relative))

    holding_changes = pd.DataFrame(holding_changes, columns =['CUSIP',
                                                              'Ticker',
                                                              'SecurityName',
                                                              'Shares2021_03_31',
                                                              'Shares2021_06_30',
                                                              'DeltaAbsolute', 
                                                              'DeltaRelative'])
    return holding_changes

print(calculate_holding_changes(buffet_holdings))

holding_changes = calculate_holding_changes(buffet_holdings)

bought = holding_changes[holding_changes['DeltaRelative'] > 0].sort_values(by=['DeltaRelative'], ascending=False)

print(bought)