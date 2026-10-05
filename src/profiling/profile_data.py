import pandas as pd
from pathlib import Path

DATA_PATH = Path("data/source/popularity.csv")

df =pd.read_csv(DATA_PATH)

print("=== DATASET SIZE ===")
print(f"Rows: {df.shape[0]:,}")
print(f"Columns: {df.shape[1]}")

print("\n=== RAW COLUMN NAMES ===")
for column in df.columns:
    print(repr(column))

print("\n=== DATA TYPES ===")
print(df.dtypes)

print("\n=== SAMPLE DATA ===")
print(df.head())

profile_df = df.copy()
profile_df.columns = profile_df.columns.str.strip()

print("\n=== NULL VALUES ===")
null_counts = profile_df.isnull().sum()
print(null_counts[null_counts > 0])

print("\n=== DUPLICATE ROWS ===")
print(f"Duplicate rows: {profile_df.duplicated().sum():,}")

print("\n=== URL UNIQUENESS ===")
print(f"Total rows: {len(profile_df):,}")
print(f"Unique URLs: {profile_df['url'].nunique():,}")
print(f"Duplicate URLs: {profile_df['url'].duplicated().sum():,}")

print("\n=== KEY COLUMN CARDINALITY ===")

columns_to_check = [
    "shares",
    "num_keywords",
    "n_tokens_content",
    "is_weekend"
]

for column in columns_to_check:
    print(f"{column}: {profile_df[column].nunique():,} unique values")

print("\n=== KEY NUMERIC RANGES ===")

numeric_columns = [
    "shares",
    "n_tokens_title",
    "n_tokens_content",
    "num_hrefs",
    "num_imgs",
    "num_videos",
    "num_keywords",
    "global_subjectivity",
    "global_sentiment_polarity"
]

for column in numeric_columns:
    print(f"\n--- {column} ---")
    print(f"Min: {profile_df[column].min()}")
    print(f"Max: {profile_df[column].max()}")
    print(f"Mean: {profile_df[column].mean():.2f}")
    print(f"Median: {profile_df[column].median():.2f}")

print("\n=== KEY PERCENTILES ===")

columns_for_percentiles = [
    "shares",
    "n_tokens_content",
    "num_imgs",
    "num_videos"
]

for column in columns_for_percentiles:
    print(f"\n--- {column} ---")
    print(
        profile_df[column].quantile(
            [0, 0.25, 0.5, 0.75, 0.95, 0.99, 1]
        )
    )

print("\n=== POTENTIAL ANOMALIES ===")

print(
    "shares <= 0:",
    (profile_df["shares"] <= 0).sum()
)

print(
    "n_tokens_content <= 0:",
    (profile_df["n_tokens_content"] <= 0).sum()
)

print(
    "global_sentiment_polarity outside [-1, 1]:",
    (
        (profile_df["global_sentiment_polarity"] < -1)
        | (profile_df["global_sentiment_polarity"] > 1)
    ).sum()
)

print("\n=== CHANNEL ONE-HOT CONSISTENCY ===")

channel_columns = [
    "data_channel_is_lifestyle",
    "data_channel_is_entertainment",
    "data_channel_is_bus",
    "data_channel_is_socmed",
    "data_channel_is_tech",
    "data_channel_is_world"
]

channel_sum = profile_df[channel_columns].sum(axis=1)

print("Exactly one channel:", (channel_sum == 1).sum())
print("No channel:", (channel_sum == 0).sum())
print("Multiple channels:", (channel_sum > 1).sum())

print("\nChannel flag values:")
for column in channel_columns:
    print(column, sorted(profile_df[column].unique()))

print("\n=== WEEKDAY ONE-HOT CONSISTENCY ===")

weekday_columns = [
    "weekday_is_monday",
    "weekday_is_tuesday",
    "weekday_is_wednesday",
    "weekday_is_thursday",
    "weekday_is_friday",
    "weekday_is_saturday",
    "weekday_is_sunday"
]

weekday_sum = profile_df[weekday_columns].sum(axis=1)

print("Exactly one weekday:", (weekday_sum == 1).sum())
print("No weekday:", (weekday_sum == 0).sum())
print("Multiple weekdays:", (weekday_sum > 1).sum())

print("\nWeekday flag values:")
for column in weekday_columns:
    print(column, sorted(profile_df[column].unique()))

# whether is_weekend is consistent with weekday flags
expected_weekend = (
    profile_df["weekday_is_saturday"]
    + profile_df["weekday_is_sunday"]
)

weekend_mismatch = (
    profile_df["is_weekend"] != expected_weekend
).sum()

print("\n=== WEEKEND CONSISTENCY ===")
print(f"is_weekend mismatch: {weekend_mismatch}")

