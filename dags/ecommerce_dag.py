from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from io import StringIO
import boto3
import psycopg2
import logging
import json
import time

# ─── Config ──────────────────────────────────────────────────────────────
bucket           = 'project-ecommerce-etl'
raw_key          = 'data/raw_data.csv'
cleaned_key      = 'data/cleaned_data.csv'
chunk_prefix     = 'data/cleaned_data_chunk'
chunk_list_key   = 'data/chunk_keys.json'
redshift_table   = 'ecommerce_sales'
iam_role         = 'arn:aws:iam::025091548078:role/RedshiftRole'
region           = 'ap-south-1'

default_args = {
    'owner': 'airflow',
    # → no retries, no retry_delay
}

# ─── Utility: save a DataFrame to S3 as CSV ───────────────────────────────
def save_df_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    logging.info(f"Saved dataframe to s3://{bucket}/{key}")

# ─── Step 1: Extract raw file from S3, re-upload as raw_data.csv ──────────
def extract_task():
    logging.info(f"Extracting data from s3://{bucket}/data/Ecommerce.csv")
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key='data/Ecommerce.csv')
    df  = pd.read_csv(obj['Body'])
    logging.info(f"Extracted {len(df)} rows")
    save_df_to_s3(df, bucket, raw_key)

# ─── Step 2: Transform → clean & upload cleaned_data.csv ──────────────────
def transform_task():
    logging.info(f"Reading raw data from s3://{bucket}/{raw_key}")
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=raw_key)
    df_raw = pd.read_csv(obj['Body'])

    logging.info("Transforming data by dropping nulls")
    df_clean = df_raw.dropna()
    logging.info(f"Transformed to {len(df_clean)} rows")

    save_df_to_s3(df_clean, bucket, cleaned_key)

# ─── Step 3a: Split cleaned CSV into ≤500-row chunks on S3 ────────────────
def split_cleaned_to_chunks(chunk_size=500):
    logging.info(f"Reading cleaned data from s3://{bucket}/{cleaned_key}")
    s3  = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=cleaned_key)
    df  = pd.read_csv(obj['Body'])

    chunks     = np.array_split(df, max(1, len(df) // chunk_size))
    chunk_keys = []

    for i, chunk in enumerate(chunks):
        chunk_key = f"{chunk_prefix}_part{i}.csv"
        logging.info(f"Uploading chunk {i} ({len(chunk)} rows) → s3://{bucket}/{chunk_key}")
        save_df_to_s3(chunk, bucket, chunk_key)
        chunk_keys.append(chunk_key)

    # Save chunk list for the loader task
    boto3.client('s3').put_object(Bucket=bucket,
                                  Key=chunk_list_key,
                                  Body=json.dumps(chunk_keys))
    logging.info(f"Wrote chunk manifest to s3://{bucket}/{chunk_list_key}")

# ─── Step 3b: Load each chunk into Redshift ───────────────────────────────
def load_chunks_to_redshift():
    s3  = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=chunk_list_key)
    chunk_keys = json.loads(obj['Body'].read().decode())

    conn = psycopg2.connect(
        dbname='dev',
        user='admin',
        password='Redminote3',
        port='5439',
        host='ecommerceworkgroup.025091548078.ap-south-1.redshift-serverless.amazonaws.com',
        sslmode='require'
    )
    conn.autocommit = True

    for key in chunk_keys:
        s3_path = f"s3://{bucket}/{key}"
        copy_sql = f"""
            COPY {redshift_table}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            CSV
            IGNOREHEADER 1
            REGION '{region}'
            LOGERRORS;
        """
        logging.info(f"COPYing {s3_path} → {redshift_table}")
        try:
            with conn.cursor() as cur:
                cur.execute(copy_sql)
            logging.info("Success")
        except Exception as e:
            logging.error(f"Failed COPY for {s3_path}: {e}")
        time.sleep(1)  # small pause to avoid throttling

    conn.close()

# ─── DAG Definition ───────────────────────────────────────────────────────
with DAG(
    dag_id='etl_ecommerce_pipeline_refactored',
    default_args=default_args,
    start_date=datetime(2025, 5, 17),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
    )

    split_chunks = PythonOperator(
        task_id='split_chunks',
        python_callable=split_cleaned_to_chunks,
        op_kwargs={'chunk_size': 500},
    )

    load_redshift = PythonOperator(
        task_id='load_redshift',
        python_callable=load_chunks_to_redshift,
        execution_timeout=timedelta(minutes=3),
    )

    extract >> transform >> split_chunks >> load_redshift
