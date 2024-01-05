import os
import re
import sqlite3
import yaml
from tempfile import TemporaryDirectory

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import Flow, task
from sqlalchemy import create_engine, Column, Integer, String, Float, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import janitor
from tqdm import tqdm
import math

# Load the configurations from YAML file
with open("conf/main.yaml") as file:
    conf = yaml.safe_load(file)


@task
def scrape_monthly_ridership_url(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    link_tag = soup.find(
        "a", {"href": lambda href: href and href.endswith(".xlsx")}
    )
    file_relative_url = link_tag["href"]
    file_absolute_url = conf["DOT_BASE_URL"] + file_relative_url
    file_name = link_tag.get_text(strip=True)
    pattern = r"(\w+ \d{4})"
    match = re.search(pattern, file_name)
    month_year = match.group(1) if match else ""
    return file_absolute_url, file_name, month_year


@task
def download_excel_workbook(file_url, output_dir):
    response = requests.get(file_url)
    file_path = os.path.join(output_dir, conf["WB_OUTPUT_NAME"])
    with open(file_path, "wb") as f:
        f.write(response.content)
    return file_path


@task
def read_excel_workbook(file_path, sheets=conf["SHEETS"]):
    xl = pd.read_excel(file_path, sheet_name=sheets)
    return xl


def read_sheet_from_excel(
    xl, sheet_name=conf["SHEET_NAME_UPT"], sheet_index=0
):
    df = xl[sheet_name].clean_names()
    print("df columns: ", df.columns)
    return df


def transform_data(df, value_name):
    """
    This function transforms the data in the dataframe 'df' according to the configuration 'conf'.
    It first drops the rows with NaN values in the 'NUMERIC_STRING_COLUMNS', then converts these columns to integer type and formats them.
    It then converts the 'CATEGORICAL_COLUMNS' to category type.
    Finally, it reshapes the dataframe to long format and splits the 'Month/Year' column into separate 'month' and 'year' columns.

    Parameters:
    df (pandas.DataFrame): The input dataframe to be transformed.
    value_name (str): The name to use for the value column when reshaping to long format.

    Returns:
    pandas.DataFrame: The transformed dataframe.
    """
    # Use the conf instead of hardcoding the columns
    numeric_string_columns = conf["NUMERIC_STRING_COLUMNS"]
    df = df.dropna(subset=numeric_string_columns)
    df[numeric_string_columns] = (
        df[numeric_string_columns].astype(int).applymap("{:05d}".format)
    )
    categorical_columns = conf["CATEGORICAL_COLUMNS"]
    df[categorical_columns] = df[categorical_columns].astype("category")
    long_data = pd.melt(
        df,
        id_vars=conf["ID_COLUMNS_MELT"],
        var_name="Month/Year",
        value_name=value_name,
    )
    long_data[["month", "year"]] = long_data["Month/Year"].str.split(
        "_", expand=True
    )
    long_data = long_data.drop(columns="Month/Year")
    long_data[["month", "year"]] = long_data[["month", "year"]].apply(
        lambda x: x.str.strip()
    )
    # Move value_name column to the end of the dataframe
    cols = list(long_data.columns.values)
    # print("cols: ", cols)
    cols.pop(cols.index(value_name))
    long_data = long_data[cols + [value_name]]
    # Show values of value_name column
    # print("value_name values: ", long_data[value_name].value_counts())
    # Ensure value_name column is the of type float
    long_data[value_name] = long_data[value_name].astype(float)

    long_data["mode"] = long_data["mode"].map(conf["MODE_MAPPING"])
    # Fill the missing values with "Unknown" for mode
    long_data["mode"] = long_data["mode"].fillna("Unknown")
    long_data["tos"] = long_data["tos"].map(conf["TOS_MAPPING"])
    # Fill the missing values with "Unknown" for type of service
    long_data["tos"] = long_data["tos"].fillna("Unknown")
    # Fill the missing values with "" for legacy ntd id
    long_data["legacy_ntd_id"] = long_data["legacy_ntd_id"].fillna("")
    # print("long_data shape: ", long_data.shape)
    # print("long_data columns: ", long_data.columns)
    # print("long_data dtypes: ", long_data.dtypes)
    return long_data


@task
def read_sheet_and_transform(xl, sheet_name, value_name):
    df = read_sheet_from_excel(xl, sheet_name)
    transformed_data = transform_data(df, value_name)
    return transformed_data


@task
def merge_transformed_data(dfs):
    """
    The function merges a list of dataframes based on specified ID columns and returns the merged
    dataframe.
    
    :param dfs: The parameter "dfs" is a list of dataframes that you want to merge together. Each
    dataframe represents transformed data from different sources or sheets
    :return: the merged dataframe.
    """
    id_vars = conf["ID_COLUMNS"]
    sheets = conf["SHEETS"]
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


# Define the SQLAlchemy base
Base = declarative_base()

mode_values = list(conf["MODE_MAPPING"].values()) + [
    "Unknown",
]
tos_values = list(conf["TOS_MAPPING"].values()) + [
    "Unknown",
]


# Define the ORM class representing the table
# The class "AgencyModeMonth" represents a table in a database that stores monthly data for different
# agencies, modes, and types of service in the transportation industry.
class AgencyModeMonth(Base):
    __tablename__ = "AgencyModeMonth"
    ntd_id = Column(Integer, primary_key=True)
    legacy_ntd_id = Column(Integer, primary_key=True)
    agency = Column(String, primary_key=True)
    status = Column(Enum(*conf["STATUS"]), primary_key=True)
    reporter_type = Column(Enum(*conf["REPORTER_TYPE"]), primary_key=True)
    uace_cd = Column(Integer, primary_key=True)
    uza_name = Column(String, primary_key=True)
    mode = Column(Enum(*mode_values), primary_key=True)
    tos = Column(Enum(*tos_values), primary_key=True)
    month = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    # The following columns are the values for each month
    # UPT Unlinked_Passenger_Trips
    UPT = Column(Float)
    # VRM Vehicle_Revenue_Miles
    VRM = Column(Float)
    # VRH Vehicle_Revenue_Hours
    VRH = Column(Float)
    # VOMS Peak_Vehicles
    VOMS = Column(Float)
    
@task
def save_data_to_database(df, db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        chunk_size = conf["CHUNK_SIZE"]
        total_rows = len(df)
        total_chunks = math.ceil(total_rows / chunk_size)

        for i in tqdm(range(total_chunks)):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df[start_idx:end_idx]

            for _, row in chunk_df.iterrows():
                agency_mode_month = AgencyModeMonth(
                    NTD_ID=row["ntd_id"],
                    Legacy_NTD_ID=row["legacy_ntd_id"],
                    Agency=row["agency"],
                    Status=row["status"],
                    Reporter_Type=row["reporter_type"],
                    UACE_CD=row["uace_cd"],
                    UZA_Name=row["uza_name"],
                    Mode=row["mode"],
                    Type_Of_Service=row["tos"],
                    Month=row["month"],
                    Year=row["year"],
                    Unlinked_Passenger_Trips=row["UPT"],
                    Vehicle_Revenue_Miles=row["VRM"],
                    Vehicle_Revenue_Hours=row["VRH"],
                    Peak_Vehicles=row["VOMS"],
                )
                session.add(agency_mode_month)

            session.commit()

        session.commit()
        print("Data saved to database successfully.")
    except Exception as e:
        session.rollback()
        print("Error occurred while saving data to database.")
        print(str(e))
    finally:
        session.close()


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
        for sheet_name in conf["SHEETS"]:
            df = read_sheet_and_transform(xl, sheet_name, sheet_name)
            dfs.append(df)
            save_data_to_intermediate_file(df, sheet_name, temp_dir)

        merged_df = merge_transformed_data(dfs)

        # Upload merged data to database
        save_data_to_database(merged_df, conf["DB_PATH"])

    # Run both subflows
    file_path = scrape_download_flow(url=conf["URL"])
    transform_merge_upload_flow(file_path)
