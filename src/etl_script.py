from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    """
    Logs progress messages with a timestamp to both the console and a log file.
    """
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    log_message = f"{timestamp} : {message}\n"
    print(log_message)
    with open("./code_log.txt", "a") as f:
        f.write(log_message)

def extract(url, table_attribs):
    """
    Extracts data from a given Wikipedia URL and returns a pandas DataFrame.
    """
    page = requests.get(url).text  # Fetch the page content
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    tables = data.find_all('tbody')
    if not tables:
        raise ValueError("No tables found on the page")
    
    table = tables[0]  # Selecting the first table
    rows = table.find_all('tr')
    
    for row in rows[1:]:  # Skipping the header row
        cols = row.find_all('td')
        if len(cols) > 1:  # Ensuring the row has data
            name = cols[1].text.strip()
            mc_usd_billion = float(cols[2].text.strip().replace(',', ''))
            
            data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion}
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df, df1], ignore_index=True)
    
    return df  

def transform(df, csv_path):
    """
    Transforms the extracted data by converting market capitalization into multiple currencies.
    """
    exchange_rate_df = pd.read_csv(csv_path)  # Read exchange rates
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    
    # Applying exchange rates to calculate market cap in different currencies
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    """
    Saves the transformed DataFrame to a CSV file.
    """
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    """
    Loads the DataFrame into an SQLite database.
    """
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    """
    Executes a SQL query and prints the results.
    """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Configuration variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_rate_csv = "exchange_rate.csv"

# Logging progress
log_progress('Preliminaries complete. Initiating ETL process')

# Extraction
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

# Transformation
Transformed_df = transform(df, exchange_rate_csv)
log_progress('Data transformation complete. Initiating loading process')

# Loading
load_to_csv(Transformed_df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(Transformed_df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Executing queries')

# Running Queries
run_query(f"SELECT * FROM {table_name} LIMIT 5", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", sql_connection)
run_query(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)

# Closing Connection
log_progress('Process Complete.')
sql_connection.close()
log_progress('Database Connection closed')
