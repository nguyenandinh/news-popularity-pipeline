import boto3

BUCKET_NAME = "news-popularity-bronze-de2026-7k3"
AWS_REGION = "ap-southeast-2"
BRONZE_PREFIX = "bronze/news_popularity/"

def create_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

from pathlib import Path
from botocore.exceptions import ClientError


def upload_batch(s3_client, file_path, batch_id):
    file_path = Path(file_path)

    # 1 Validate local file
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    # 2 Build S3 object key
    object_key = (
        f"{BRONZE_PREFIX}"
        f"batch_id={batch_id}/"
        f"{file_path.name}"
    )

    # 3 Check whether object already exists
    try:
        s3_client.head_object(
            Bucket=BUCKET_NAME,
            Key=object_key
        )

        print(f"SKIPPED: {object_key} already exists")
        return "SKIPPED"

    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code not in ("404", "NoSuchKey", "NotFound"):
            raise

    # 4. Upload new object
    try:
        with open(file_path, "rb") as file:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=object_key,
                Body=file,
                ContentType="text/csv",
                IfNoneMatch="*"
            )

    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code in ("PreconditionFailed", "412"):
            print(f"SKIPPED: {object_key} already exists")
            return "SKIPPED"

        if error_code in ("ConditionalRequestConflict", "409"):
            print(f"CONFLICT: Concurrent upload detected for {object_key}")

        raise

    print(f"UPLOADED: {object_key}")
    return "UPLOADED"

def main():
    s3_client = create_s3_client()

    batch_ids = ["20260101", "20260102", "20260103"]

    for batch_id in batch_ids:
        file_path = (
            f"data/generated/batch_{batch_id}.csv"
        )

        upload_batch(
            s3_client=s3_client,
            file_path=file_path,
            batch_id=batch_id
        )


if __name__ == "__main__":
    main()