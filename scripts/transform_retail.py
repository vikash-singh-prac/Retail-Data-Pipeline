import pandas as pd
import sys

sys.path.append('../')

def clean_online_retail(csv_path: str, output_path: str) -> None:


    df  =pd.read_csv(csv_path)

    df.columns = df.columns.str.strip()

    df = df.dropna(subset=['InvoiceNo','StockCode','InvoiceDate'])

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')

    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month
    df['Day'] = df['InvoiceDate'].dt.day
    df['Hour'] = df['InvoiceDate'].dt.hour

    print(df.head())

    df.to_csv(output_path, index=False)


if __name__=="__main__":
    input_path= '/opt/airflow/data/OnlineRetail.csv'
    output_path = '/tmp/online_retail_cleaned.csv'
    clean_online_retail(input_path, output_path)
