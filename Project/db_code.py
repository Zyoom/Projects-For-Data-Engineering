import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db') #Connection to our Database

table_name = 'INSTRUCTOR' #Creating a Table for Data

attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE'] #Table Attributes

file_path = '/Users/davidpedroza/Desktop/DataEngineering/Project' #pwd Path
df = pd.read_csv(file_path, names = attribute_list) #read csv files command 

