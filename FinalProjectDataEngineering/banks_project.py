import logging
import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime


# Function to log progress at various stages
def log_progress(message):
    logging.basicConfig(filename='code_log.txt', level=logging.INFO, format='%(asctime)s - %(message)s')
    logging.info(message)


# Function to extract data from the specified URL
def extract():
    log_progress("Starting data extraction")
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Locate the table by its class name
    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')

    # Extract the relevant data from the table
    data = []
    for row in rows[1:11]:  # Process only the top 10 rows
        cols = row.find_all('td')
        name = cols[1].get_text(strip=True)  # Extract bank name
        mc_usd = float(cols[2].get_text(strip=True).replace(',', ''))  # Extract market capitalization in USD
        data.append([name, mc_usd])

    # Create a DataFrame from the extracted data
    df = pd.DataFrame(data, columns=['Name', 'MC_USD_Billion'])
    log_progress("Data extraction completed")
    return df


# Function to transform the extracted data using exchange rates
def transform(df):
    log_progress("Starting data transformation")

    # Load exchange rates from the provided CSV file
    exchange_rates = pd.read_csv(
        'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv')

    # Print the columns to debug
    print("Exchange Rates Columns:", exchange_rates.columns)

    # Extract exchange rates for GBP, EUR, and INR
    usd_to_gbp = exchange_rates.loc[exchange_rates['Currency'] == 'GBP', 'Rate'].values[0]
    usd_to_eur = exchange_rates.loc[exchange_rates['Currency'] == 'EUR', 'Rate'].values[0]
    usd_to_inr = exchange_rates.loc[exchange_rates['Currency'] == 'INR', 'Rate'].values[0]

    # Calculate market capitalizations in GBP, EUR, and INR
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * usd_to_gbp).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * usd_to_eur).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * usd_to_inr).round(2)

    log_progress("Data transformation completed")
    return df


# Function to load the transformed data into a CSV file
def load_to_csv(df, path):
    log_progress("Starting data load to CSV")
    df.to_csv(path, index=False)
    log_progress("Data load to CSV completed")


# Function to load the transformed data into a SQLite database
def load_to_db(df, db_name, table_name):
    log_progress("Starting data load to database")
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    log_progress("Data load to database completed")


# Function to run queries on the database and fetch results
def run_queries(db_name, query):
    log_progress("Running query: " + query)
    conn = sqlite3.connect(db_name)
    result = pd.read_sql_query(query, conn)
    conn.close()
    log_progress("Query execution completed")
    return result


# Main execution
if __name__ == "__main__":
    # Extract data from the web page
    df_extracted = extract()
    print("Extracted Data:")
    print(df_extracted)

    # Transform the extracted data
    df_transformed = transform(df_extracted)
    print("Transformed Data:")
    print(df_transformed)

    # Load the transformed data into a CSV file
    load_to_csv(df_transformed, './Largest_banks_data.csv')

    # Load the transformed data into a SQLite database
    load_to_db(df_transformed, 'Banks.db', 'Largest_banks')

    # Example query to fetch all data from the database
    query = "SELECT * FROM Largest_banks"
    result = run_queries('Banks.db', query)
    print("Query Result:")
    print(result)

    # Verify log entries by reading the log file
    with open('code_log.txt', 'r') as log_file:
        log_contents = log_file.read()
        print("Log Contents:")
        print(log_contents)
