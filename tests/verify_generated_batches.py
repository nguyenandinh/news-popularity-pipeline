import pandas as pd
from pathlib import Path

# Configuration

DATA_DIR = Path("data/generated")

weekday_columns = [
    "weekday_is_monday",
    "weekday_is_tuesday",
    "weekday_is_wednesday",
    "weekday_is_thursday",
    "weekday_is_friday",
    "weekday_is_saturday",
    "weekday_is_sunday" 
]

# Verify each generated batch 
for file_path in sorted(DATA_DIR.glob("batch_*.csv")):
    df = pd.read_csv(file_path)

    duplicate_count = df["url"].duplicated().sum()
    invalid_shares = (df["shares"] <= 0).sum()
    invalid_sentiment = (~df["global_sentiment_polarity"].between(-1, 1)).sum()
    invalid_weekday = (df[weekday_columns].sum(axis=1) != 1).sum()

    print(f"\n=== {file_path.name} ===")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Duplicate URLs: {duplicate_count}")
    print(f"Invalid shares: {invalid_shares}")
    print(f"Invalid sentiment: {invalid_sentiment}")
    print(f"Invalid weekday: {invalid_weekday}")

    assert len(df) == 1000
    assert len(df.columns) == 63
    assert duplicate_count == 5
    assert invalid_shares == 5
    assert invalid_sentiment == 5
    assert invalid_weekday == 5

print("\nALL GENERATED BATCHES VERIFIED")