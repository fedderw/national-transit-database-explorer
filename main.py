import requests
from bs4 import BeautifulSoup
import re
from prefect import task, Flow
import pandas as pd
import sqlite3

@task
def scrape_monthly_ridership_url(url):
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the <a> tag containing the link to the .xlsx file
    link_tag = soup.find('a', {'href': lambda href: href.endswith('.xlsx')})
    
    # Extract the href attribute (file URL) and text (file name)
    file_url = link_tag['href']
    file_name = link_tag.get_text(strip=True)
    
    # Extract the month and year from the file name
    pattern = r'(\w+ \d{4})'
    match = re.search(pattern, file_name)
    if match:
        month_year = match.group(1)
    else:
        month_year = ''
    
    return file_url, file_name, month_year

@task
def read_sheet_from_excel(file_url):
    # Read the 'UTP' sheet from the Excel file
    df = pd.read_excel(file_url, sheet_name='UTP')
    return df

@task
def transform_data(df):
    # Reshape the data from wide to long format
    long_data = pd.melt(
        df,
        id_vars=['NTD ID', 'Legacy NTD ID', 'Agency', 'Status', 'Reporter Type', 'UZA', 'UZA Name', 'Mode', 'TOS'],
        var_name='Month/Year',
        value_name='Ridership'
    )
    
    # Separate the "Month/Year" column into "Month" and "Year" columns
    long_data[['Month', 'Year']] = long_data['Month/Year'].str.split('/', expand=True)
    
    # Remove any leading/trailing whitespaces from the new columns
    long_data['Month'] = long_data['Month'].str.strip()
    long_data['Year'] = long_data['Year'].str.strip()
    
    # Remove any leading/trailing whitespaces from the 'Ridership' column
    long_data['Ridership'] = long_data['Ridership'].str.replace(',', '').str.strip().astype(float)
    
    # Map the full values for 'Mode' column
    mode_mapping = {
        'AB': 'Articulated Buses',
        'AO': 'Automobiles',
        'AR': 'Alaska Railroad',
        'BR': 'Over-the-Road Buses',
        'BU': 'Buses',
        'CC': 'Cable Car',
        'MB': 'Bus',
        'RB': 'Bus Rapid Transit',
        'TR': 'Aerial Tramway',
        'CR': 'Commuter Rail',
        'LR': 'Light Rail',
        'HR': 'Heavy Rail',
        'DR': 'Demand Response',
        'FB': 'Ferryboat',
        'VP': 'Vanpool',
        'EB': 'Trolleybus or Electric Bus',
        'MO': 'Monorail',
        'PT': 'Paratransit'
    }
    long_data['Mode'] = long_data['Mode'].map(mode_mapping)
    
    # Map the full values for 'TOS' column
    tos_mapping = {
        'DO': 'Direct Operations',
        'PT': 'Purchased Transportation'
    }
    long_data['TOS'] = long_data['TOS'].map(tos_mapping)
    
    return long_data

@task
def save_data_to_database(df):
    # Connect to the SQLite database
    conn = sqlite3.connect('ridership.db')
    cursor = conn.cursor()
    
    # Create the Ridership table with more informative column names
    cursor.execute('''
        CREATE TABLE Ridership (
            NTD_ID TEXT,
            Legacy_NTD_ID TEXT,
            Agency TEXT,
            Status TEXT,
            Reporter_Type TEXT,
            UZA INTEGER,
            UZA_Name TEXT,
            Mode TEXT,
            Type_Of_Service TEXT,
            Month TEXT,
            Year INTEGER,
            Ridership REAL
        )
    ''')
    
    # Insert records into the Ridership table
    records = df.to_records(index=False)
    cursor.executemany('INSERT INTO Ridership VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', records)
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

# URL to scrape
url = 'https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release'

# Create a Prefect Flow
with Flow("Scrape and Transform Ridership Data") as flow:
    file_url, file_name, month_year = scrape_monthly_ridership_url(url)
    df = read_sheet_from_excel(file_url)
    transformed_data = transform_data(df)
    save_data_to_database(transformed_data)

