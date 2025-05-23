# import boto3
# import pandas as pd
# from io import StringIO

# def load_to_s3(df, bucket_name, key):
#     s3 = boto3.client('s3')
#     csv_buffer = StringIO()
#     df.to_csv(csv_buffer, index=False)
#     s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())

# def load_to_redshift(bucket_path, redshift_table, iam_role, delimiter=','):
#     import psycopg2
#     conn = psycopg2.connect(
#         dbname='dev',
#         user='vimal',
#         password='vimal',
#         port='5439',
#         host='ecommerceworkgroup.025091548078.ap-south-1.redshift-serverless.amazonaws.com'
#     )
#     cursor = conn.cursor()
#     copy_command = f"""
#         COPY {redshift_table}
#         FROM '{bucket_path}'
#         IAM_ROLE '{iam_role}'
#         FORMAT AS CSV
#         IGNOREHEADER 1
#         TIMEFORMAT 'auto'
#         DELIMITER '{delimiter}'
#         REMOVEQUOTES
#         EMPTYASNULL
#         BLANKSASNULL;
#     """

#     cursor.execute(copy_command)
#     conn.commit()
#     cursor.close()
#     conn.close()


# def load_data(df, bucket_name, s3_key, redshift_table, iam_role):
#     load_to_s3(df, bucket_name, s3_key)
#     s3_path = f's3://{bucket_name}/{s3_key}'
#     load_to_redshift(s3_path, redshift_table, iam_role)


# import boto3
# import psycopg2
# import os

# def load_to_s3(df, bucket, key):
#     csv_buffer = df.to_csv(index=False)
#     s3 = boto3.client('s3')
#     s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer)

# def load_to_redshift(bucket, key, table, iam_role, region='us-east-1'):
#     s3_path = f's3://{bucket}/{key}'
#     conn = psycopg2.connect(
#         dbname='dev',
#         user='vimal',
#         password='vimal',
#         port='5439',
#         host='ecommerceworkgroup.025091548078.ap-south-1.redshift-serverless.amazonaws.com'
#     )
#     cursor = conn.cursor()
#     copy_sql = f"""
#         COPY {table}
#         FROM '{s3_path}'
#         IAM_ROLE '{iam_role}'
#         CSV
#         IGNOREHEADER 1
#         REGION '{region}';
#     """
#     cursor.execute(copy_sql)
#     conn.commit()
#     cursor.close()
#     conn.close()

# def load_data(df, bucket, cleaned_key, redshift_table, iam_role):
#     # Upload cleaned CSV to S3
#     load_to_s3(df, bucket, cleaned_key)

#     # Load data from S3 to Redshift
#     load_to_redshift(bucket, cleaned_key, redshift_table, iam_role)



import boto3
import psycopg2
import numpy as np
from io import StringIO

def load_to_s3_in_chunks(df, bucket, key_prefix, chunk_size=10000):
    s3 = boto3.client('s3')
    chunk_keys = []
    chunks = np.array_split(df, len(df) // chunk_size + 1)
    for i, chunk in enumerate(chunks):
        csv_buffer = StringIO()
        chunk.to_csv(csv_buffer, index=False)
        chunk_key = f"{key_prefix}_part{i}.csv"
        s3.put_object(Bucket=bucket, Key=chunk_key, Body=csv_buffer.getvalue())
        chunk_keys.append(chunk_key)
    return chunk_keys

def load_to_redshift_chunked(bucket, chunk_keys, table, iam_role, region='us-east-1'):
    s3_path_template = f's3://{bucket}/{{}}'
    conn = psycopg2.connect(
        dbname='dev',
        user='vimal',
        password='vimal',
        port='5439',
        host='ecommerceworkgroup.025091548078.ap-south-1.redshift-serverless.amazonaws.com'
    )
    cursor = conn.cursor()
    for key in chunk_keys:
        s3_path = s3_path_template.format(key)
        copy_sql = f"""
            COPY {table}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            CSV
            IGNOREHEADER 1
            REGION '{region}';
        """
        cursor.execute(copy_sql)
        conn.commit()
    cursor.close()
    conn.close()

def load_data(df, bucket, cleaned_key_prefix, redshift_table, iam_role, chunk_size=10000):
    # Upload data in chunks to S3
    chunk_keys = load_to_s3_in_chunks(df, bucket, cleaned_key_prefix, chunk_size=chunk_size)

    # Load each chunk from S3 to Redshift
    load_to_redshift_chunked(bucket, chunk_keys, redshift_table, iam_role)
