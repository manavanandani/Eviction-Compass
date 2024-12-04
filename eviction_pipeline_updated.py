from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
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

        # Clean Column Names
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

# Converts the data to a format suitable for Snowflake
def load_to_snowflake(**kwargs):
    ti = kwargs["ti"]
    processed_csv_key = ti.xcom_pull(task_ids="etl_and_store_csv")

    # Read the CSV from S3
    s3_hook = S3Hook(aws_conn_id="aws_s3")
    csv_file = s3_hook.read_key(
        key=processed_csv_key, bucket_name="california-evictions-data-lake"
    )

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

# Task 3: Load data to Snowflake (Convert to Snowflake format data)
load_to_snowflake_task = PythonOperator(
    task_id="load_to_snowflake",
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag,
)

# Task 4: Create_database_and_schema
create_database_and_schema_task = SnowflakeOperator(
    task_id="create_database_and_schema",
    snowflake_conn_id="snowflake_conn",
    sql="""
    CREATE DATABASE IF NOT EXISTS eviction_data;
    USE DATABASE eviction_data;
    CREATE SCHEMA IF NOT EXISTS eviction_schema;
    USE SCHEMA eviction_schema;
    """,
    dag=dag,
)

# Task 5: Create Staging Table
create_staging_table_task = SnowflakeOperator(
    task_id="create_staging_table",
    snowflake_conn_id="snowflake_conn",
    sql="""

    USE DATABASE eviction_data;
    USE SCHEMA eviction_schema;

    CREATE OR REPLACE TABLE eviction_staging (
        eviction_id VARCHAR(50),
        address VARCHAR(255),
        zip VARCHAR(10),
        file_date DATE,
        non_payment BOOLEAN,
        breach BOOLEAN,
        nuisance BOOLEAN,
        illegal_use BOOLEAN,
        failure_to_sign_renewal BOOLEAN,
        access_denial BOOLEAN,
        unapproved_subtenant BOOLEAN,
        owner_move_in BOOLEAN,
        demolition BOOLEAN,
        capital_improvement BOOLEAN,
        substantial_rehab BOOLEAN,
        ellis_act_withdrawal BOOLEAN,
        condo_conversion BOOLEAN,
        roommate_same_unit BOOLEAN,
        other_cause BOOLEAN,
        late_payments BOOLEAN,
        lead_remediation BOOLEAN,
        development BOOLEAN,
        good_samaritan_ends BOOLEAN,
        supervisor_district INTEGER,
        neighborhood VARCHAR(100),
        data_loaded_at TIMESTAMP,
        latitude FLOAT,
        longitude FLOAT,
        eviction_year INTEGER
    );
    """,
    dag=dag,
)

# Task 6: S3 Integartion to Snowflake & Loaded data to snowflake

"""
Task Details:
- Configures Snowflake to use a specific database and schema.
- Creates a link (integration) between Snowflake and S3 bucket using AWS IAM credentials.
- Loads data from a specific location in the S3 bucket into a staging table in Snowflake, using a dynamic path and CSV file format.
"""

load_staging_table_task = SnowflakeOperator(
    task_id="load_staging_table",
    snowflake_conn_id="snowflake_conn",
    sql="""

    USE DATABASE eviction_data;
    USE SCHEMA eviction_schema;

    CREATE STORAGE INTEGRATION my_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_IAM_USER_ARN = 'arn:aws:iam::619071339710:role/warehouse-project-iam-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://california-evictions-data-lake/Transformed Data/*')
    STORAGE_BLOCKED_LOCATIONS = ()

    COPY INTO eviction_staging
    FROM 's3://california-evictions-data-lake/Transformed Data/{{ ti.xcom_pull(task_ids='load_to_snowflake') }}'
    STORAGE_INTEGRATION = my_s3_integration
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
    """,
    dag=dag,
)


# Task 7: Create Dimension Tables: Address_Dimension, Cause_Dimension, Time_Dimension 
"""
Task Details:
- Extracts unique address, cause, and time information from the eviction_staging table.
- Structures the data into three separate tables to make it easier to analyze eviction data:
    - Address_Dimension focuses on location.
    - Cause_Dimension focuses on eviction reasons.
    - Time_Dimension focuses on date-based information.
"""
create_dimension_tables_task = SnowflakeOperator(
    task_id="create_dimension_tables",
    snowflake_conn_id="snowflake_conn",
    sql="""

    USE DATABASE eviction_data;
    USE SCHEMA eviction_schema;

    -- Create Address Dimension table
    CREATE OR REPLACE TABLE Address_Dimension AS
    SELECT 
        HASH(address, zip) AS address_key,
        address,
        zip,
        neighborhood,
        supervisor_district,
        latitude,
        longitude
    FROM (
        SELECT DISTINCT
            address,
            zip,
            neighborhood,
            supervisor_district,
            latitude,
            longitude
        FROM eviction_staging
    );

    -- Create Cause Dimension table
    CREATE OR REPLACE TABLE Cause_Dimension AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS cause_key,
        failure_to_sign_renewal,
        access_denial,
        unapproved_subtenant,
        owner_move_in,
        demolition,
        capital_improvement,
        substantial_rehab,
        ellis_act_withdrawal,
        condo_conversion,
        roommate_same_unit,
        other_cause,
        late_payments,
        lead_remediation,
        development,
        good_samaritan_ends
    FROM (
        SELECT DISTINCT
            failure_to_sign_renewal,
            access_denial,
            unapproved_subtenant,
            owner_move_in,
            demolition,
            capital_improvement,
            substantial_rehab,
            ellis_act_withdrawal,
            condo_conversion,
            roommate_same_unit,
            other_cause,
            late_payments,
            lead_remediation,
            development,
            good_samaritan_ends
        FROM eviction_staging
    );

    -- Create Time Dimension table
    CREATE OR REPLACE TABLE Time_Dimension AS
    SELECT 
        HASH(file_date) AS time_key,
        file_date,
        YEAR(file_date) AS eviction_year,
        MONTH(file_date) AS month,
        DAY(file_date) AS day,
        QUARTER(file_date) AS quarter
    FROM (
        SELECT DISTINCT file_date
        FROM eviction_staging
    );
    """,
    dag=dag,
)

# Task 8: Create fact table 
"""
Task Details:
- The fact table combines data from the eviction_staging table with the Address_Dimension, Cause_Dimension, and Time_Dimension tables.
- Each join ensures the fact table has foreign keys (address_key, cause_key, time_key) to link back to these dimension tables.
"""
create_fact_table_task = SnowflakeOperator(
    task_id="create_fact_table",
    snowflake_conn_id="snowflake_conn",
    sql="""

    USE DATABASE eviction_data;
    USE SCHEMA eviction_schema;

    CREATE OR REPLACE TABLE Eviction_Facts AS
    SELECT
        s.eviction_id,
        a.address_key, 
        c.cause_key,
        t.time_key,
        s.non_payment,
        s.breach,
        s.nuisance,
        s.illegal_use
    FROM eviction_staging s
    JOIN Address_Dimension a 
        ON s.address = a.address AND s.zip = a.zip
    JOIN Cause_Dimension c 
        ON (
            s.failure_to_sign_renewal = c.failure_to_sign_renewal AND
            s.access_denial = c.access_denial AND
            s.unapproved_subtenant = c.unapproved_subtenant AND
            s.owner_move_in = c.owner_move_in AND
            s.demolition = c.demolition AND
            s.capital_improvement = c.capital_improvement AND
            s.substantial_rehab = c.substantial_rehab AND
            s.ellis_act_withdrawal = c.ellis_act_withdrawal AND
            s.condo_conversion = c.condo_conversion AND
            s.roommate_same_unit = c.roommate_same_unit AND
            s.other_cause = c.other_cause AND
            s.late_payments = c.late_payments AND
            s.lead_remediation = c.lead_remediation AND
            s.development = c.development AND
            s.good_samaritan_ends = c.good_samaritan_ends
        )
    JOIN Time_Dimension t 
        ON s.file_date = t.file_date;
    """,
    dag=dag,
)

# Set task dependencies
fetch_sfo_eviction_data_task >> etl_and_store_csv_task >> load_to_snowflake_task >> create_database_and_schema_task >> create_staging_table_task >> load_staging_table_task >> create_dimension_tables_task >> create_fact_table_task