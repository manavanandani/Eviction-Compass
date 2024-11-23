from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import os
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


bucket_name = "california-evictions-data-lake"

def fetch_sfo_eviction_data():
    api_url = "https://data.sfgov.org/resource/5cei-gny5.json"
    limit, offset, all_data = 1000, 0, []

    try:
        while True:
            response = requests.get(
                api_url,
                params={"$limit": limit, "$offset": offset, "$order": "file_date DESC"},
            )
            if response.status_code != 200:
                print(f"Failed: {response.status_code}, {response.text[:500]}")
                break

            data = response.json()
            all_data.extend(data)
            if len(data) < limit:
                break
            offset += limit

        # Create a timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_key = f"Raw Data/Real-time Data/sfo_evictions_{timestamp}.json"

        # Convert data to JSON string
        json_data = json.dumps(all_data, indent=4)

        # Upload to S3
        s3_hook = S3Hook(aws_conn_id="aws_s3")
        bucket_name = "california-evictions-data-lake"

        s3_hook.load_string(
            string_data=json_data, key=json_key, bucket_name=bucket_name, replace=True
        )

        print(f"Data uploaded to S3://{bucket_name}/{json_key}")
        return json_key
    except Exception as e:
        print(f"Error: {e}")
        raise


def etl_and_store_csv(bucket_name):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3")

        # List all objects in the Raw Data/Real-time Data/ prefix
        prefix = "Raw Data/Real-time Data/"
        s3_objects = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

        # Filter for JSON files and sort by last modified date
        json_files = [obj for obj in s3_objects if obj.endswith(".json")]
        latest_file = max(
            json_files, key=lambda obj: s3_hook.get_key(obj, bucket_name).last_modified
        )

        print(f"Latest file found: {latest_file}")

        # Fetch JSON data from S3
        raw_json = s3_hook.read_key(key=latest_file, bucket_name=bucket_name)
        if not raw_json:
            raise ValueError(f"No data found at S3://{bucket_name}/{latest_file}")

        json_data = json.loads(raw_json)

        # Load JSON data into Pandas DataFrame
        df = pd.DataFrame(json_data)
        print(f"Loaded DataFrame with {len(df)} rows and {len(df.columns)} columns")

        # Transformation 1: Clean Column Names
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        print(df.isnull())

        df = df.drop(["city", "state", "constraints_date", "data_as_of"], axis=1)

        # Clean the zip column
        def clean_zip(zip_code):
            if pd.isna(zip_code) or len(str(zip_code)) < 5:
                return "00000"
            elif len(str(zip_code)) > 5:
                return str(zip_code)[:5]
            else:
                return str(zip_code)

        df["zip"] = df["zip"].apply(clean_zip)

        df["supervisor_district"] = df["supervisor_district"].fillna(-1)

        df["neighborhood"] = df["neighborhood"].fillna("Unknown")

        # Extract latitude and longitude from `client_location`
        df["latitude"] = df["client_location"].apply(
            lambda x: x["latitude"] if isinstance(x, dict) else None
        )
        df["longitude"] = df["client_location"].apply(
            lambda x: x["longitude"] if isinstance(x, dict) else None
        )
        df = df.drop(columns=["client_location", "shape"])

        def clean_coordinate(coord):
            try:
                if coord is None or coord == "":
                    return None  # Return None for missing or empty coordinates
                return round(
                    float(coord), 6
                )  # Convert to float and round to 6 decimal places
            except (ValueError, TypeError):
                return None  # Return None if conversion fails

        df["latitude"] = df["latitude"].apply(clean_coordinate)
        df["longitude"] = df["longitude"].apply(clean_coordinate)

        df = df.dropna(subset=["latitude", "longitude", "address"])

        # Convert Date Columns to Datetime
        date_columns = ["file_date", "data_loaded_at"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Add Year of Eviction
        df["eviction_year"] = df["file_date"].dt.year

        # Normalize Neighborhood Names
        df["neighborhood"] = df["neighborhood"].str.title()

        # Convert the transformed DataFrame to CSV
        csv_data = df.to_csv(index=False)

        # Generate the S3 key for the processed CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        processed_csv_key = f"Transformed Data/sfo_evictions_processed_{timestamp}.csv"

        # Upload the processed CSV to S3
        s3_hook.load_string(
            string_data=csv_data,
            key=processed_csv_key,
            bucket_name=bucket_name,
            replace=True,
        )

        print(f"Processed CSV uploaded to S3://{bucket_name}/{processed_csv_key}")

        return processed_csv_key
    except Exception as e:
        print(f"Error in etl_and_store_csv: {str(e)}")
        raise


def load_to_snowflake(**kwargs):
    ti = kwargs["ti"]
    processed_csv_key = ti.xcom_pull(task_ids="etl_and_store_csv")

    # Read the CSV from S3
    s3_hook = S3Hook(aws_conn_id="aws_s3")
    csv_file = s3_hook.read_key(
        key=processed_csv_key, bucket_name="california-evictions-data-lake"
    )
    # df = pd.read_csv(pd.compat.StringIO(csv_file))
    df = pd.read_csv(StringIO(csv_file))

    print(df.head())

    # Convert DataFrame to list of tuples
    data = [tuple(x) for x in df.to_numpy()]

    # Return the data and column names
    return {"data": data, "columns": df.columns.tolist()}


dag = DAG(
    "sfo_eviction_dag",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
    catchup=False,
)

# Task 1: Fetch sfo-eviction-realtime-data and stored in s3 datalake
fetch_sfo_eviction_data_task = PythonOperator(
    task_id="fetch_sfo_eviction_data",
    python_callable=fetch_sfo_eviction_data,
    dag=dag,
)

# Task 2: Perform ETL and store CSV in S3
etl_and_store_csv_task = PythonOperator(
    task_id="etl_and_store_csv",
    python_callable=etl_and_store_csv,
    op_kwargs={
        "json_key": "{{ ti.xcom_pull(task_ids='fetch_json') }}",
        "bucket_name": bucket_name,
    },
)

# Task 3: Load data to Snowflake
load_to_snowflake_task = PythonOperator(
    task_id="load_to_snowflake",
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag,
)

# Task 4: Create Snowflake tables
create_tables_task = SnowflakeOperator(
    task_id="create_snowflake_tables",
    snowflake_conn_id="snowflake_conn",
    sql=""" 
    USE DATABASE eviction_database;
    USE SCHEMA eviction_database_schema;

    CREATE TABLE IF NOT EXISTS DIM_DATE (
        DATE_ID INT PRIMARY KEY,
        CALENDAR_DATE DATE,
        DATE DATE,
        YEAR INT,
        MONTH INT,
        DAY INT,
        WEEK INT,
        QUARTER INT
    );

    CREATE TABLE IF NOT EXISTS DIM_EVICTION_REASONS (
        REASON_ID INT PRIMARY KEY,
        REASON_NAME STRING
    );

    CREATE TABLE IF NOT EXISTS DIM_NEIGHBORHOOD (
        NEIGHBORHOOD_ID INT PRIMARY KEY,
        NEIGHBORHOOD_NAME STRING,
        SUPERVISOR_DISTRICT INT
    );

    CREATE TABLE IF NOT EXISTS FACT_EVICTIONS (
        EVICTION_ID STRING PRIMARY KEY,
        NEIGHBORHOOD_ID INT,
        DATE_ID INT,
        LATITUDE FLOAT,
        LONGITUDE FLOAT,
        EVICTION_YEAR INT,
        REASON_ID INT,
        FOREIGN KEY (NEIGHBORHOOD_ID) REFERENCES DIM_NEIGHBORHOOD(NEIGHBORHOOD_ID),
        FOREIGN KEY (DATE_ID) REFERENCES DIM_DATE(DATE_ID),
        FOREIGN KEY (REASON_ID) REFERENCES DIM_EVICTION_REASONS(REASON_ID)
    );
    """,
    dag=dag,
)

# Task 5: Insert data to Snowflake
insert_data_task = SnowflakeOperator(
    task_id="insert_snowflake_data",
    snowflake_conn_id="snowflake_conn",
    sql="""
    USE DATABASE EVICTION_DATABASE;
    USE SCHEMA EVICTION_DATABASE_SCHEMA;

    -- Insert into DIM_DATE
    INSERT INTO DIM_DATE (DATE_ID, CALENDAR_DATE, DATE, YEAR, MONTH, DAY, WEEK, QUARTER)
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(FILE_DATE, 'YYYYMMDD')) AS DATE_ID,
        FILE_DATE AS CALENDAR_DATE,
        FILE_DATE AS DATE,
        YEAR(FILE_DATE) AS YEAR,
        MONTH(FILE_DATE) AS MONTH,
        DAY(FILE_DATE) AS DAY,
        WEEKOFYEAR(FILE_DATE) AS WEEK,
        QUARTER(FILE_DATE) AS QUARTER
    FROM {{ ti.xcom_pull(task_ids='load_to_snowflake')['data'] }};

    -- Insert into DIM_EVICTION_REASONS
    INSERT INTO DIM_EVICTION_REASONS (REASON_ID, REASON_NAME)
    SELECT 
        ROW_NUMBER() OVER (ORDER BY REASON) AS REASON_ID,
        REASON AS REASON_NAME
    FROM (
        SELECT 'NON_PAYMENT' AS REASON FROM {{ ti.xcom_pull(task_ids='load_to_snowflake')['data'] }} WHERE NON_PAYMENT = TRUE
        UNION
        SELECT 'BREACH' WHERE BREACH = TRUE
        UNION
        SELECT 'NUISANCE' WHERE NUISANCE = TRUE
        -- Add other reasons here
    );

    -- Insert into DIM_NEIGHBORHOOD
    INSERT INTO DIM_NEIGHBORHOOD (NEIGHBORHOOD_ID, NEIGHBORHOOD_NAME, SUPERVISOR_DISTRICT)
    SELECT 
        ROW_NUMBER() OVER (ORDER BY NEIGHBORHOOD) AS NEIGHBORHOOD_ID,
        NEIGHBORHOOD AS NEIGHBORHOOD_NAME,
        SUPERVISOR_DISTRICT
    FROM {{ ti.xcom_pull(task_ids='load_to_snowflake')['data'] }}
    GROUP BY NEIGHBORHOOD, SUPERVISOR_DISTRICT;

    -- Insert into FACT_EVICTIONS
    INSERT INTO FACT_EVICTIONS (EVICTION_ID, NEIGHBORHOOD_ID, DATE_ID, LATITUDE, LONGITUDE, EVICTION_YEAR, REASON_ID)
    SELECT
        E.EVICTION_ID,
        N.NEIGHBORHOOD_ID,
        TO_NUMBER(TO_CHAR(E.FILE_DATE, 'YYYYMMDD')) AS DATE_ID,
        E.LATITUDE,
        E.LONGITUDE,
        E.EVICTION_YEAR,
        R.REASON_ID
    FROM {{ ti.xcom_pull(task_ids='load_to_snowflake')['data'] }} E
    JOIN DIM_NEIGHBORHOOD N ON E.NEIGHBORHOOD = N.NEIGHBORHOOD_NAME
    JOIN DIM_EVICTION_REASONS R ON 
        (E.NON_PAYMENT = TRUE AND R.REASON_NAME = 'NON_PAYMENT') OR
        (E.BREACH = TRUE AND R.REASON_NAME = 'BREACH') OR
        (E.NUISANCE = TRUE AND R.REASON_NAME = 'NUISANCE')
        -- Add other reason checks here
    ;
    """,
    dag=dag,
)


# Set dependencies 
(
    fetch_sfo_eviction_data_task
    >> etl_and_store_csv_task
    >> load_to_snowflake_task
    >> create_tables_task
    >> insert_data_task
)
