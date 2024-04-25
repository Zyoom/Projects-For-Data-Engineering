import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

# Helper method to read CSV files
def execute_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# Helper method to read JSON Files
def execute_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

# Extracts from XML files but parse first through element tree
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns= ['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(file_to_process)