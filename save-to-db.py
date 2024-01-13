import psycopg2
#connect to database
from sec_api import QueryApi
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="example")

# initialize the query API
queryApi = QueryApi(api_key="200a1dcbf7e6da4ca9a4f0feadbc15b0f0d8df0c45c1ec0a98dea11005b76b0d")

def get_13f_filings(start=0):
    print(f"Getting next 13F batch starting at {start}")
    
    query = {
      "query": { "query_string": { 
          "query": "formType:\"13F-HR\" AND NOT formType:\"13F-HR/A\" AND periodOfReport:\"2021-06-30\"" 
        } },
      "from": start,
      "size": "10",
      "sort": [{ "filedAt": { "order": "desc" } }]
    }

    response = queryApi.get_filings(query)

    return response['filings']

# fetch the 10 most recent 13F filings
filings_batch = get_13f_filings(10)

# load all holdings of the first 13F filing into a pandas dataframe 
holdings_example = pd.json_normalize(filings_batch[0]['holdings'])

def save_to_db(filings):
    cur = conn.cursor()

    for filing in filings:
        if len(filing['holdings']) == 0:
            continue
        
        insert_commands = (
            """
                INSERT INTO filings (
                    filing_id, 
                    cik,
                    filer_name,
                    period_of_report
                ) 
                VALUES (%s, %s, %s, %s)
            """,
            """
                INSERT INTO holdings (
                    filing_id, 
                    name_of_issuer,
                    cusip,
                    title_of_class,
                    value,
                    shares,
                    put_call
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        )
        
        filing_values = (
            filing['id'],
            filing['cik'], 
            filing['companyName'].upper(), # convert all names to upper case to make unique search easier
            filing['periodOfReport']
        )
        
        cur.execute(insert_commands[0], filing_values)
        
        for holding in filing['holdings']:       
            holding_values = (
                filing['id'], 
                holding['nameOfIssuer'].upper(), # convert all names to upper case to make unique search easier
                holding['cusip'],
                holding['titleOfClass'],
                holding['value'],
                holding['shrsOrPrnAmt']['sshPrnamt'],
                holding['putCall'] if 'putCall' in holding else '',
            )
                    
            cur.execute(insert_commands[1], holding_values)
        
    cur.close()
    conn.commit()
save_to_db(filings=filings_batch)