import requests
from bs4 import BeautifulSoup
import re
from prefect import task, Flow
import pandas as pd
import sqlite3
from pprint import pprint as print
import asyncio
import aiohttp


@task
def scrape_monthly_ridership_url(url):
    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the <a> tag containing the link to the .xlsx file
    link_tag = soup.find(
        "a", {"href": lambda href: href and href.endswith(".xlsx")}
    )

    # Extract the href attribute (file URL) and text (file name)
    # "https://www.transit.dot.gov/sites/fta.dot.gov/files/2023-05/March%202023%20Complete%20Monthly%20Ridership%20%28with%20adjustments%20and%20estimates%29.xlsx"
    file_relative_url = link_tag["href"]
    print(f"URL found: {file_relative_url}")
    file_absolute_url = "https://www.transit.dot.gov/" + file_relative_url
    print(file_absolute_url)
    file_name = link_tag.get_text(strip=True)
    # Extract the month and year from the file name
    pattern = r"(\w+ \d{4})"
    match = re.search(pattern, file_name)
    if match:
        month_year = match.group(1)
    else:
        month_year = ""
    print(f"file_name: {file_name}")
    print(f"month_year: {month_year}")

    return file_absolute_url, file_name, month_year


@task
def read_excel_workbook(file_url, sheets=["UPT", "VRM", "VRH", "VOMS"]):
    # Read the Excel file
    print(f"Reading Excel file from '{file_url}'")
    xl = pd.read_excel(file_url, sheet_name=sheets)
    return xl


def read_sheet_from_excel(xl, sheet_name="UPT", sheet_index=0):
    # Read the sheet into a DataFrame
    print(f"Reading '{sheet_name}' sheet")
    df = xl[sheet_name]
    return df


def transform_data(df, value_name):
    print(f"Transforming data for '{value_name}'")
    numeric_string_columns = [
        "NTD ID",
        "UZA",
    ]
    # Drop any rows with missing values for the numeric string columns
    df.dropna(subset=numeric_string_columns, inplace=True)
    # Convert the numeric columns to strings without decimal points
    for col in numeric_string_columns:
        df[col] = df[col].apply(lambda x: f"{int(x):05d}" if pd.notna(x) else x)

    # Encode the 'Status', 'Reporter Type', 'Mode', and 'TOS' columns as categories
    for col in ["Status", "Reporter Type", "Mode", "TOS"]:
        df[col] = df[col].astype("category")

    # Reshape the data from wide to long format
    long_data = pd.melt(
        df,
        id_vars=[
            "NTD ID",
            "Legacy NTD ID",
            "Agency",
            "Status",
            "Reporter Type",
            "UZA",
            "UZA Name",
            "Mode",
            "TOS",
        ],
        var_name="Month/Year",
        value_name=value_name,
    )

    # Separate the "Month/Year" column into "Month" and "Year" columns
    long_data[["Month", "Year"]] = long_data["Month/Year"].str.split(
        "/", expand=True
    )
    # Drop the "Month/Year" column
    long_data.drop(columns="Month/Year", inplace=True)
    # Remove any leading/trailing whitespaces from the new columns
    long_data["Month"] = long_data["Month"].str.strip()
    long_data["Year"] = long_data["Year"].str.strip()

    # Map the full values for 'Mode' column
    mode_mapping = {
        "AB": "Articulated Buses",
        "AO": "Automobiles",
        "AR": "Alaska Railroad",
        "BR": "Over-the-Road Buses",
        "BU": "Buses",
        "CC": "Cable Car",
        "MB": "Bus",
        "RB": "Bus Rapid Transit",
        "TR": "Aerial Tramway",
        "CR": "Commuter Rail",
        "LR": "Light Rail",
        "HR": "Heavy Rail",
        "DR": "Demand Response",
        "FB": "Ferryboat",
        "VP": "Vanpool",
        "EB": "Trolleybus or Electric Bus",
        "MO": "Monorail",
        "PT": "Paratransit",
    }
    long_data["Mode"] = long_data["Mode"].map(mode_mapping)

    # Map the full values for 'TOS' column
    tos_mapping = {"DO": "Direct Operations", "PT": "Purchased Transportation"}
    long_data["TOS"] = long_data["TOS"].map(tos_mapping)

    return long_data


@task
def read_upt_sheet_from_excel(xl):
    df = read_sheet_from_excel(xl, sheet_name="UPT", sheet_index=0)
    transformed_data = transform_data(df, "UPT")
    print(f"transformed_data.shape: {transformed_data.shape}")
    return transformed_data


@task
def read_vrm_sheet_from_excel(xl):
    df = read_sheet_from_excel(xl, sheet_name="VRM", sheet_index=1)
    transformed_data = transform_data(df, "VRM")
    print(f"transformed_data.shape: {transformed_data.shape}")
    return transformed_data


@task
def read_vrh_sheet_from_excel(xl):
    df = read_sheet_from_excel(xl, sheet_name="VRH", sheet_index=2)
    transformed_data = transform_data(df, "VRH")
    print(f"transformed_data.shape: {transformed_data.shape}")
    return transformed_data


@task
def read_voms_sheet_from_excel(xl):
    df = read_sheet_from_excel(xl, sheet_name="VOMS", sheet_index=3)
    transformed_data = transform_data(df, "VOMS")
    print(f"transformed_data.shape: {transformed_data.shape}")
    return transformed_data


@task
def merge_transformed_data(dfs):
    id_vars = [
        "NTD ID",
        "Legacy NTD ID",
        "Agency",
        "Status",
        "Reporter Type",
        "UZA",
        "UZA Name",
        "Mode",
        "TOS",
        "Month",
        "Year",
    ]

    print("Merging data")
    merged_df = None

    for name, df in dfs.items():
        if merged_df is None:
            merged_df = df.merge(df[id_vars + [name]], on=id_vars, how="left")
        else:
            merged_df = merged_df.merge(
                df[id_vars + [name]], on=id_vars, how="left"
            )

        print(f"merged_df.shape: {merged_df.shape}")

    return merged_df


@task
def save_data_to_intermediate_file(df, sheet_name):
    """Save the data to an intermediate CSV file"""
    print("Saving data to intermediate file")
    df.to_csv(f"data/{sheet_name}.csv", index=False)


@task
def save_data_to_database(df):
    """Save the data to a SQLite database"""
    # Connect to the SQLite database
    print("Connecting to the database")
    conn = sqlite3.connect("data/ntd.db")
    print("Connected to the database")
    cursor = conn.cursor()

    # Create table with more informative column names
    print("Creating MonthlyData table")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MonthlyData (
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
            Unlinked_Passenger_Trips REAL,
            Vehicle_Revenue_Miles REAL,
            Vehicle_Revenue_Hours REAL,
            Peak_Vehicles REAL,
            PRIMARY KEY (NTD_ID, Legacy_NTD_ID, Agency, Status, Reporter_Type, UZA, UZA_Name, Mode, Type_Of_Service, Month, Year)
        );
    """)

    # Insert records into the MonthlyData table
    records = df.to_records(index=False)
    print("Inserting records into the MonthlyData table")
    cursor.executemany(
        "INSERT INTO MonthlyData VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        records,
    )

    # Commit the changes and close the connection
    print("Committing changes and closing the connection")
    conn.commit()
    conn.close()
    print("Done")


# URL to scrape
url = "https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release"


# Create a Prefect Flow
@Flow
def flow():
    file_url, file_name, month_year = scrape_monthly_ridership_url(url)
    print(f"file_url: {file_url}")
    print(f"file_name: {file_name}")
    print(f"month_year: {month_year}")
    dfs = {}
    xl = read_excel_workbook(file_url)
    dfs["UPT"] = read_upt_sheet_from_excel(xl)
    dfs["VRM"] = read_vrm_sheet_from_excel(xl)
    dfs["VRH"] = read_vrh_sheet_from_excel(xl)
    dfs["VOMS"] = read_voms_sheet_from_excel(xl)
    # Save the data to an intermediate CSV file
    [save_data_to_intermediate_file(dfs[key], key) for key in dfs.keys()]
    merged_data = merge_transformed_data(dfs)
    save_data_to_database(merged_data)


# Run the flow
flow()
