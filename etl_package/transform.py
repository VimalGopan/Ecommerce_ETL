import numpy as np
import pandas as pd

def transform_data(df):
    df.drop_duplicates(inplace=True)
    df.dropna(subset=['Description', 'CustomerID'], inplace=True)

    df['CustomerID'] = df['CustomerID'].astype(int)
    df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]
    df.drop(columns=['InvoiceDate'], inplace=True)

    df['Description'] = df['Description'].str.strip().str.lower()
    df['StockCode'] = df['StockCode'].str.strip().str.upper()
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

    df = df[['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'UnitPrice', 'CustomerID', 'Country', 'TotalPrice']]

    df.reset_index(drop=True, inplace=True)
    return df
