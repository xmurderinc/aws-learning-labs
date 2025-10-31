import json
import os
import numpy as np
import pandas as pd
from faker import Faker
import boto3
from datetime import datetime


def lambda_handler(event, context):  
    fake = Faker()

    # Number of sample rows
    row_count = 100

    names = [fake.name() for _ in range(row_count)]

    # Generate emails based on names
    emails = [f"{name.split()[0].lower()}.{name.split()[1].lower()}@example.com" for name in names]


    sample_data = {
        'Customer': names,
        'Age': np.random.randint(18, 65, size=row_count),
        'City': [fake.city() for _ in range(row_count)],
        'Email':emails,
        'Order Quantity': np.random.randint(0, 10, size=row_count),
        'Price': np.round(np.random.uniform(0.0, 100.0, size=row_count), 2)
    }


    # Write file locally in /tmp
    os.chdir("/tmp")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"test_{timestamp}.txt"


    with open (f"/tmp/{s3_key}","w") as file:
        file.write(pd.DataFrame(sample_data).to_string())

    # Upload to S3
    s3 = boto3.client('s3')
    bucket_name = "my-lambda-stack"  # <-- replace with your bucket name
    s3.upload_file(f"/tmp/{s3_key}", bucket_name, s3_key)


    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Sample data generated and uploaded to s3://{bucket_name}/{s3_key}",
            # "location": ip.text.replace("\n", "")
        }),
    }
