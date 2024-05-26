import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup


# Adjust pandas display options for better readability
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)        # Set the width of the display
pd.set_option('display.colheader_justify', 'center')  # Center the column headers
pd.set_option('display.col_space', 20)      # Set minimum column width

# URL to scrape
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'

# File paths and database names
csv_path = '/Users/davidpedroza/Desktop/Projects-For-Data-Engineering/WebscrapingPractice/top_50_films.csv'
db_name = 'Movies.db'
table_name = 'Top_50'

# Initialize an empty DataFrame with the required columns
df = pd.DataFrame(columns=['Average Rank', 'Film', 'Year'])

# Initialize counter to keep track of the number of records processed
count = 0

# Request the web page content
html_page = requests.get(url).text
# Parse the HTML content with BeautifulSoup
data = BeautifulSoup(html_page, 'html.parser')

# Find all table bodies in the HTML
tables = data.find_all('tbody')

# Get all rows from the first table body
rows = tables[0].find_all('tr')

# Iterate through each row in the table
for row in rows:
    if count < 50:  # Limit to top 50 films
        # Find all columns in the row
        col = row.find_all('td')
        if len(col) != 0:  # Ensure the row has columns
            # Create a dictionary from the column data
            data_dict = {
                'Average Rank': col[0].contents[0],
                'Film': col[1].contents[0],
                'Year': col[2].contents[0]
            }
            # Create a temporary DataFrame from the dictionary
            df1 = pd.DataFrame(data_dict, index=[0])
            # Concatenate the temporary DataFrame with the main DataFrame
            df = pd.concat([df, df1], ignore_index=True)
            # Increment the counter
            count += 1
    else:
        break  # Exit the loop if 50 records have been processed

# Print the DataFrame to verify the data
print(df)

# Save the DataFrame to a CSV file
df.to_csv(csv_path)

# Save the DataFrame to an SQLite database
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
