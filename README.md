# Project overview
This is a pipeline that scrapes the url for the most recent National Transit Database excel workbook, and transforms it into a local sqlite database

## Libraries Used
- os
- re
- sqlite3
- yaml
- tempfile
- pandas
- requests
- bs4 (BeautifulSoup)
- prefect
- sqlalchemy
- janitor
- tqdm
- math

## Configuration
- Configurations are loaded from a YAML file `conf/main.yaml`.

## Main Functionalities

### Scraping
- `scrape_monthly_ridership_url(url)`: Scrapes the given URL to find and return the absolute URL, file name, and month-year of the ridership Excel file.

### Downloading
- `download_excel_workbook(file_url, output_dir)`: Downloads the Excel workbook from the provided URL to the specified output directory.

### Reading and Transforming Data
- `read_excel_workbook(file_path, sheets)`: Reads the specified sheets from an Excel workbook.
- `read_sheet_from_excel(xl, sheet_name, sheet_index)`: Reads a specific sheet from the Excel workbook.
- `transform_data(df, value_name)`: Transforms the data in the dataframe according to the configurations specified.

### Merging and Saving Data
- `merge_transformed_data(dfs)`: Merges a list of dataframes based on specified ID columns.
- `save_data_to_intermediate_file(df, sheet_name, output_dir)`: Saves the provided dataframe to an intermediate Parquet file.

### Database Operations
- Class `AgencyModeMonth`: Represents a table in a database for storing monthly ridership data.
- `save_data_to_database(df, db_path)`: Saves the dataframe to a SQLite database.
- `transform_to_star_schema(db_path)`: Transforms the data into a star schema for database storage.

### Prefect Flows
- `scrape_download_flow(url)`: A Prefect flow for scraping and downloading data.
- `transform_merge_upload_flow(file_path)`: A Prefect flow for transforming, merging, and uploading data.

## Usage
1. Ensure all required libraries are installed.
2. Configure the `conf/main.yaml` file as per requirements.
3. Run the script to perform the data scraping, transformation, and storage operations.

## Note
- This script requires a proper configuration file and external data sources to function correctly.
- It is designed to handle specific data formats and may need adjustments for different data structures.
