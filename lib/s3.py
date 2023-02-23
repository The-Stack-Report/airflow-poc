from airflow.models import Variable
import boto3
from io import StringIO

from datetime import timedelta, datetime
import boto3
import botocore

from lib.date_picker import missing_dates, create_datetime

session = boto3.session.Session()

FOLDER_PREFIX = "dev/"

ACCESS_ID = Variable.get("AWS_ACCESS_ID")
ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
DAILY_BUCKET_URL = Variable.get("DAILY_BUCKET_URL")

def upload_to_bucket(bucket_name, key, df, **kwargs): # This should send the file to S3 bucket, now just confirms the file is there
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_client = session.client("s3",
            region_name="ams3",
            endpoint_url=DAILY_BUCKET_URL,
            aws_access_key_id=ACCESS_ID,
            aws_secret_access_key=ACCESS_KEY
        )
        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=f"{FOLDER_PREFIX}{key}.csv"
        )
    except Exception as e:
        print("Exception when connecting to S3: ", e)
        exit(1)

def get_existing_files():
    s3 = boto3.client("s3",
            config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
            region_name="ams3",
            endpoint_url=DAILY_BUCKET_URL,
            aws_access_key_id=ACCESS_ID,
            aws_secret_access_key=ACCESS_KEY
        )

    response = s3.list_objects(Bucket='the-stack-report-prototyping', Prefix=FOLDER_PREFIX, Delimiter='/')
    dates = []
    for obj in response['Contents']:
        date_str = obj['Key'].split("/")[-1].replace(".csv", "")
        dates.append(create_datetime(date_str))

    print(dates)

    return dates

def get_missing_dates(sdate, edate):
    files = get_existing_files()
    missing = missing_dates(create_datetime(sdate), create_datetime(edate), files)

    return missing


