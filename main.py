import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import sqlite3
import os
from tempfile import TemporaryDirectory
from prefect import task, Flow


@task
def scrape_monthly_ridership_url(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    link_tag = soup.find(
        "a", {"href": lambda href: href and href.endswith(".xlsx")}
    )
    file_relative_url = link_tag["href"]
    file_absolute_url = "https://www.transit.dot.gov/" + file_relative_url
    file_name = link_tag.get_text(strip=True)
    pattern = r"(\w+ \d{4})"
    match = re.search(pattern, file_name)
    month_year = match.group(1) if match else ""
    return file_absolute_url, file_name, month_year


@task
def download_excel_workbook(file_url, output_dir):
    response = requests.get(file_url)
    file_path = os.path.join(output_dir, "ntd.xlsx")
    with open(file_path, "wb") as f:
        f.write(response.content)
    return file_path


@task
def read_excel_workbook(file_path, sheets=["UPT", "VRM", "VRH", "VOMS"]):
    xl = pd.read_excel(file_path, sheet_name=sheets)
    return xl


def read_sheet_from_excel(xl, sheet_name="UPT", sheet_index=0):
    df = xl[sheet_name]
    return df


def transform_data(df, value_name):
    numeric_string_columns = ["NTD ID", "UZA"]
    df = df.dropna(subset=numeric_string_columns)
    df[numeric_string_columns] = (
        df[numeric_string_columns].astype(int).applymap("{:05d}".format)
    )
    categorical_columns = ["Status", "Reporter Type", "Mode", "TOS"]
    df[categorical_columns] = df[categorical_columns].astype("category")
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
    long_data[["Month", "Year"]] = long_data["Month/Year"].str.split(
        "/", expand=True
    )
    long_data = long_data.drop(columns="Month/Year")
    long_data[["Month", "Year"]] = long_data[["Month", "Year"]].apply(
        lambda x: x.str.strip()
    )
    # Move value_name column to the end of the dataframe
    cols = list(long_data.columns.values)
    cols.pop(cols.index(value_name))
    long_data = long_data[cols + [value_name]]

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
    tos_mapping = {"DO": "Direct Operations", "PT": "Purchased Transportation"}
    long_data["TOS"] = long_data["TOS"].map(tos_mapping)
    return long_data


@task
def read_sheet_and_transform(xl, sheet_name, value_name):
    df = read_sheet_from_excel(xl, sheet_name)
    transformed_data = transform_data(df, value_name)
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
    sheets = ["UPT", "VRM", "VRH", "VOMS"]
    merged_df = None
    for df in dfs:
        if merged_df is None:
            merged_df = df
        else:
            merged_df = merged_df.merge(df, on=id_vars, how="left")
    return merged_df


@task
def save_data_to_intermediate_file(df, sheet_name, output_dir):
    output_path = os.path.join(output_dir, f"{sheet_name}.parquet")
    df.to_parquet(output_path, index=False)


@task
def save_data_to_database(df, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS AgencyModeMonth (
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
    """
    )
    # Update the table if

    # records
    records = df.to_records(index=False)
    cursor.executemany(
        "INSERT INTO AgencyModeMonth VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        records,
    )
    conn.commit()
    conn.close()


# Create a temporary directory for intermediate files
with TemporaryDirectory() as temp_dir:
    # Create subflow for scraping/downloading data
    @Flow
    def scrape_download_flow(url):
        # Scrape URL
        file_url, file_name, month_year = scrape_monthly_ridership_url(url)
        # Download and save Excel workbook
        file_path = download_excel_workbook(file_url, temp_dir)
        return file_path

    # Create subflow for transforming, merging, and uploading data
    @Flow
    def transform_merge_upload_flow(file_path):
        # Read Excel workbook
        xl = read_excel_workbook(file_path)
        # Transform and merge data
        dfs = []
        for sheet_name in ["UPT", "VRM", "VRH", "VOMS"]:
            df = read_sheet_and_transform(xl, sheet_name, sheet_name)
            dfs.append(df)
            save_data_to_intermediate_file(df, sheet_name, temp_dir)

        merged_df = merge_transformed_data(dfs)

        # Upload merged data to database
        save_data_to_database(merged_df, "data/ntd.db")

    # Run both subflows
    file_path = scrape_download_flow(
        url="https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release"
    )
    transform_merge_upload_flow(file_path)
