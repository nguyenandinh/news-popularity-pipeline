import pandas as pd
from pathlib import Path

# ==================================================
# CONFIGURATION
# ==================================================

SOURCE_PATH = Path("data/source/popularity.csv")
SAMPLE_SIZE = 1000
BATCH_ID = "20260101"
RANDOM_SEED = 42

# ==================================================
# BATCH GENERATION
# ==================================================

def generate_batch(batch_id, sample_size, random_seed):
    """
    Generate a synthetic news batch using historical records.

    Args:
        batch_id: Logical date identifier for the batch.
        sample_size: Number of records to generate.
        random_seed: Seed for reproducible sampling.
    """

    # 1 Load historical source data
    df = pd.read_csv(SOURCE_PATH)

    # 2 Standardize column names 
    df.columns = df.columns.str.strip()

    # 3 Sample historical records as templates
    sample_df = df.sample(
        n=sample_size,
        replace=True,
        random_state=random_seed
    ).copy()

    synthetic_df = sample_df.reset_index(drop=True)

    # 4 Assign unique synthetic article IDs
    synthetic_df["url"] = [
        f"synthetic://article/{batch_id}/{i:06d}"
        for i in range(1, sample_size + 1)
    ]

    # 5 Add batch metadata
    synthetic_df["batch_id"] = batch_id

    # Use a fixed timestamp for reproducible test data
    synthetic_df["ingestion_timestamp"] = pd.to_datetime(
        batch_id,
        format="%Y%m%d",
        utc=True
    )

    # 6 Inspect clean synthetic batch
    print("=== SYNTHETIC BATCH ===")
    print(f"Rows: {len(synthetic_df):,}")
    print(f"Columns: {len(synthetic_df.columns)}")

    print("\n=== SAMPLE ===")
    print(
        synthetic_df[
            [
                "url",
                "n_tokens_content",
                "num_imgs",
                "global_sentiment_polarity",
                "shares",
                "batch_id"
            ]
        ].head()
    )
    # 7 Verify clean batch integrity
    print("\n=== CLEAN BATCH CHECK ===")
    print(f"Unique URLs: {synthetic_df['url'].nunique():,}")
    print(f"Duplicate URLs: {synthetic_df['url'].duplicated().sum():,}")
    print(f"Null URLs: {synthetic_df['url'].isna().sum():,}")

    # --------------------------------
    # Inject controlled data-quality errors
    # --------------------------------

    # 8.1 Inject duplicate URLs
    duplicate_indices = range(0, 5)

    for i in duplicate_indices:
        synthetic_df.loc[i, "url"]  = synthetic_df.loc[i + 100, "url"]

    # 8.2 Inject negative share counts
    share_indices = range(5, 10)

    synthetic_df.loc[
        list(share_indices),
        "shares"
    ] = -100

    # 8.3 Inject out-of-range sentiment values
    sentiment_indices = range(10, 15)

    synthetic_df.loc[
        list(sentiment_indices),
        "global_sentiment_polarity"
    ] = 2.0  

    # 8.4 Inject invalid weekdays flags
    weekday_indices = list(range(15, 20))

    weekday_columns = [
        "weekday_is_monday",
        "weekday_is_tuesday",
        "weekday_is_wednesday",
        "weekday_is_thursday",
        "weekday_is_friday",
        "weekday_is_saturday",
        "weekday_is_sunday"
    ]
    # Reset all weekday flags 
    synthetic_df.loc[weekday_indices, weekday_columns] = 0

    # Activate two weekday flags to violate the one-hot rule
    synthetic_df.loc[weekday_indices,"weekday_is_monday"] = 1
    synthetic_df.loc[weekday_indices,"weekday_is_tuesday"] = 1

    # ==================================================
    # ERROR INJECTION VERIFICATION
    # ==================================================

    # 9 Verify injected data quality issues
    print("\n=== INJECTED ERROR CHECK ===")

    duplicate_count = synthetic_df["url"].duplicated(keep=False).sum()

    negative_shares_count = (
        synthetic_df["shares"] <= 0
    ).sum()

    invalid_sentiment_count = (
        (synthetic_df["global_sentiment_polarity"] < -1)
        |
        (synthetic_df["global_sentiment_polarity"] > 1)
    ).sum()

    weekday_sum = synthetic_df[weekday_columns].sum(axis=1)

    invalid_weekday_count = (
        weekday_sum != 1
    ).sum()

    print(f"Rows involved in duplicate URLs: {duplicate_count}")
    print(f"Invalid shares: {negative_shares_count}")
    print(f"Invalid sentiment: {invalid_sentiment_count}")
    print(f"Invalid weekday: {invalid_weekday_count}")

    # ==================================================
    # EXPORT SYNTHETIC BATCH
    # ==================================================

    # 10. Export synthetic batch to CSV
    OUTPUT_DIR = Path("data/generated")

    # Create directory if it doesn't exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Define output file path
    OUTPUT_PATH = OUTPUT_DIR / f"batch_{batch_id}.csv"

    # Save synthetic data
    synthetic_df.to_csv(
        OUTPUT_PATH,
        index=False
    )

    print("\n=== OUTPUT ===")
    print(f"Saved batch to: {OUTPUT_PATH}")
    print(f"Records saved: {len(synthetic_df):,}")

    return synthetic_df

# ==================================================
# MAIN ENTRY POINT
# ==================================================

if __name__ == "__main__":
    batch_ids = [
        "20260101",
        "20260102",
        "20260103"]
    for i, batch_id in enumerate(batch_ids):
        generate_batch(
            batch_id=batch_id,
            sample_size=SAMPLE_SIZE,
            random_seed=RANDOM_SEED + i
        )