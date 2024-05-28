import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')  #Connection to our Database

table_name = 'INSTRUCTOR'  #Creating a Table for Data

attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']  #Table Attributes

file_path = '/Users/davidpedroza/Desktop/DataEngineering/Project'  #pwd Path
df = pd.read_csv(file_path, names=attribute_list)  #read csv files command

df.to_sql(table_name, conn, if_exists='replace', index=False)
print('Table is ready')

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {'ID': [100],
             'FNAME': ['John'],
             'LNAME': ['Doe'],
             'CITY': ['Paris'],
             'CCODE': ['FR']}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists='append', index=False)
print('Data appended successfully')

conn.close()
