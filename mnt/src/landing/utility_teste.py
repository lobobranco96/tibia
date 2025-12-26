import os
from datetime import datetime
import boto3
import logging

logger = logging.getLogger(__name__)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

class CSVLanding:

    def __init__(self):
        self.today = datetime.today()
        self.bucket = "lakehouse"
        self.prefix = "landing"

        self.s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )

    def write(self, df, category_dir, dataset_name):
        partition = f"year={self.today:%Y}/month={self.today:%m}/day={self.today:%d}"

        key = (
            f"{self.prefix}/"
            f"{partition}/"
            f"{category_dir}/"
            f"{dataset_name}.csv"
        )

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, encoding="utf-8")

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="text/csv"
        )

        logger.info(f"Arquivo salvo na Landing: s3a://{self.bucket}/{key}")

        return {
            "path": f"s3a://{self.bucket}/{key}",
            "rows": len(df),
            "columns": df.columns.tolist(),
            "timestamp": datetime.now().isoformat()
        }