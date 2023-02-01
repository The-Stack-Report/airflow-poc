from airflow.models import Variable
import boto3
from io import StringIO

session = boto3.session.Session()


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
            Key=f"{key}.csv"
        )

    except Exception as e:
        print("Exception when connecting to S3: ", e)
        exit(1)