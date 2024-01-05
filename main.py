import os
import re
import sqlite3
import yaml
from tempfile import TemporaryDirectory

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import Flow, task
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Enum,
    Table,
    MetaData,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.automap import automap_base
from database_models import (
    Base,
    Agency,
    Status,
    ReporterType,
    UrbanizedArea,
    Mode,
    TypeOfService,
    AgencyModeMonth,
)
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
    # print("df columns: ", df.columns)
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
    df.loc[:, numeric_string_columns] = (
        df[numeric_string_columns]
        .astype(int)
        .apply(lambda x: x.apply("{:05d}".format))
    )
    categorical_columns = conf["CATEGORICAL_COLUMNS"]
    df.loc[:, categorical_columns] = df[categorical_columns].astype("category")
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
    long_data.loc[:, ["month", "year"]] = long_data[["month", "year"]].apply(
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
    long_data.loc[:, value_name] = long_data[value_name].astype(float)

    long_data.loc[:, "mode"] = long_data["mode"].map(conf["MODE_MAPPING"])
    # Fill the missing values with "Unknown" for mode
    long_data.loc[:, "mode"] = long_data["mode"].fillna("Unknown")
    long_data.loc[:, "tos"] = long_data["tos"].map(conf["TOS_MAPPING"])
    # Fill the missing values with "Unknown" for type of service
    long_data.loc[:, "tos"] = long_data["tos"].fillna("Unknown")
    # Fill the missing values with "" for legacy ntd id
    long_data.loc[:, "legacy_ntd_id"] = long_data["legacy_ntd_id"].fillna("")
    # Drop legacy ntd id column
    # long_data = long_data.drop(columns="legacy_ntd_id")
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


@task
def create_and_populate_database(df, db_path):
    """
    This function creates the database and populates the dimension tables and fact table.
    """
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        # Populate dimension tables and create a mapping for foreign keys
        agency_mapping = {}
        for ntd_id, agency_name in (
            df[["ntd_id", "agency"]].drop_duplicates().itertuples(index=False)
        ):
            agency_obj = Agency(ntd_id=ntd_id, agency_name=agency_name)
            session.add(agency_obj)
            agency_mapping[agency_name] = ntd_id

        status_mapping = {}
        for status in df["status"].unique():
            status_obj = Status(status=status)
            session.add(status_obj)
            session.flush()  # To get the generated ID
            status_mapping[status] = status_obj.status_id

        reporter_mapping = {}
        for reporter_type in df["reporter_type"].unique():
            reporter_obj = ReporterType(reporter_type=reporter_type)
            session.add(reporter_obj)
            session.flush()  # To get the generated ID
            reporter_mapping[reporter_type] = reporter_obj.reporter_id

        uza_mapping = {}
        for uace_cd, uza_name in (
            df[["uace_cd", "uza_name"]]
            .drop_duplicates()
            .itertuples(index=False)
        ):
            uza_obj = UrbanizedArea(uace_cd=uace_cd, uza_name=uza_name)
            session.add(uza_obj)
            uza_mapping[
                uace_cd
            ] = uace_cd  # uace_cd is the primary key and uza_id

        mode_mapping = {}
        for mode in df["mode"].unique():
            mode_obj = Mode(mode=mode)
            session.add(mode_obj)
            session.flush()  # To get the generated ID
            mode_mapping[mode] = mode_obj.mode_id

        tos_mapping = {}
        for tos in df["tos"].unique():
            tos_obj = TypeOfService(tos=tos)
            session.add(tos_obj)
            session.flush()  # To get the generated ID
            tos_mapping[tos] = tos_obj.tos_id
        print("Committing dimension tables to database...")
        session.commit()
        print("Dimension tables committed to database.")
        # Make sure we're only using the columns we need
        df = df[
            [
                "ntd_id",
                "status",
                "reporter_type",
                "uace_cd",
                "mode",
                "tos",
                "month",
                "year",
                "UPT",
                "VRM",
                "VRH",
                "VOMS",
            ]
        ]

        # Insert data into fact table
        print("Inserting records into fact table...")
        for record in tqdm(df.to_dict(orient="records")):
            # record['ntd_id'] = agency_mapping[record['agency']]
            try:
                record["status_id"] = status_mapping[record["status"]]
                record["reporter_id"] = reporter_mapping[record["reporter_type"]]
                # record['uace_cd'] = uza_mapping[record['uace_cd']]  # uace_cd is both the key and the value
                record["mode_id"] = mode_mapping[record["mode"]]
                record["tos_id"] = tos_mapping[record["tos"]]
                # Now that we have the foreign keys, we can drop the original columns
                del record["status"]
                del record["reporter_type"]
                del record["mode"]
                del record["tos"]
                session.add(AgencyModeMonth(**record))
            except Exception as e:
                print(e)
                print(record)
                raise e
        print("Committing records to database...")
        session.commit()
        print("Records committed to database.")


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
        create_and_populate_database(merged_df, conf["DB_PATH"])

    # Run both subflows
    file_path = scrape_download_flow(url=conf["URL"])
    transform_merge_upload_flow(file_path)
