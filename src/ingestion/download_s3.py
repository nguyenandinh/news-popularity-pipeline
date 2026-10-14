import argparse
from pathlib import Path

import boto3


BUCKET_NAME = "news-popularity-bronze-de2026-7k3"
AWS_REGION = "ap-southeast-2"


def download_batch(batch_id: str, profile_name: str | None = None) -> Path:
    s3_key = (
        f"bronze/news_popularity/"
        f"batch_id={batch_id}/batch_{batch_id}.csv"
    )

    output_path = Path("data/generated") / f"batch_{batch_id}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    session = boto3.Session(
        profile_name=profile_name,
        region_name=AWS_REGION,
    )

    s3 = session.client("s3")

    print(f"Downloading s3://{BUCKET_NAME}/{s3_key}")

    s3.download_file(
        BUCKET_NAME,
        s3_key,
        str(output_path),
    )

    print(f"Download completed: {output_path}")
    print(f"File size: {output_path.stat().st_size} bytes")

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Download a news popularity batch from S3 Bronze"
    )

    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--profile", default=None)

    args = parser.parse_args()

    download_batch(
        batch_id=args.batch_id,
        profile_name=args.profile,
    )


if __name__ == "__main__":
    main()