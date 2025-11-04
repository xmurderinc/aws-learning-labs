import boto3
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os

def lambda_handler(event, context):
    # ==============================
    # 1️⃣ EXTRACT: Grab latest CSV from S3
    # ==============================
    bucket_name = "my-lambda-stack"
    prefix = "data/"  # Adjust to your folder in S3

    s3 = boto3.client('s3')

    # List CSV files in S3
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    txt_files = [obj for obj in response.get('Contents', []) if obj['Key'].endswith(".txt") or obj['Key'].endswith(".csv")]

    if not txt_files:
        raise Exception("No TXT/CSV files found in the S3 bucket/prefix!")

    latest_file = max(txt_files, key=lambda x: x['LastModified'])
    latest_key = latest_file['Key']

    obj = s3.get_object(Bucket=bucket_name, Key=latest_key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))  # CSV recommended

    # ==============================
    # 2️⃣ TRANSFORM: Clean & Enrich
    # ==============================
    df = df.dropna()
    for col in ["Age", "Order Quantity", "Price"]:
        df = df[df[col] != 0]

    df["Email"] = df["Email"].str.lower()
    df["City"] = df["City"].str.title()
    df["Total"] = df["Order Quantity"] * df["Price"]

    # ==============================
    # 3️⃣ LOAD: Push to AWS RDS
    # ==============================
    db_url = os.environ['DB_URL']  # Use Lambda env variable for security
    engine = create_engine(db_url)

    df.to_sql("customer_orders_clean", engine, index=False, if_exists="replace")

    # ==============================
    # 4️⃣ ANALYTICS: Simple Visualization
    # ==============================
    city_summary = df.groupby("City")["Total"].sum().reset_index().sort_values(by="Total", ascending=False)
    top_cities = city_summary.head(10)

    plt.figure(figsize=(10,6))
    plt.bar(top_cities["City"], top_cities["Total"], color="skyblue")
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 10 Cities by Total Sales")
    plt.ylabel("Total Sales")
    plt.tight_layout()

    # Save chart to /tmp and upload to S3 (Lambda cannot show plots)
    chart_path = "/tmp/top_cities.png"
    plt.savefig(chart_path)
    s3.upload_file(chart_path, bucket_name, f"analytics/top_cities_{latest_key.split('/')[-1].replace('.csv','.png')}")

    return {
        "statusCode": 200,
        "body": f"ETL complete, data loaded and chart saved for {latest_key}"
    }
