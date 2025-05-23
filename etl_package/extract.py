import pandas as pd
import boto3
from io import StringIO

def extract_data(bucket_name, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    body = response['Body'].read().decode('ISO-8859-1')
    df = pd.read_csv(StringIO(body))
    return df
