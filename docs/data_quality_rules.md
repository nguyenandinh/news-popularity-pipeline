# Data Quality Rules

## Dataset
Online News Popularity

Baseline:
- 39,644 records
- 61 columns
- 0 null values
- 0 duplicate rows
- 39,644 unique URLs

## Standardization Rules

| Field | Rule |
|---|---|
| Column names | Remove leading/trailing whitespace |
| Channel flags | Convert one-hot columns into `channel` |
| Weekday flags | Convert one-hot columns into `weekday` |

## Validation Rules

| Field | Rule | Action if violated |
|---|---|---|
| url | Must not be null | Quarantine |
| url | Must be unique within processed data | Quarantine duplicate |
| shares | Must be > 0 | Quarantine |
| Count fields | Must be >= 0 | Quarantine |
| global_sentiment_polarity | Must be between -1 and 1 | Quarantine |
| Weekday flags | Values must be 0 or 1 | Quarantine |
| Weekday flags | Exactly one weekday must be active | Quarantine |
| is_weekend | Must match Saturday/Sunday | Quarantine |
| Channel flags | Values must be 0 or 1 | Quarantine |
| Channel flags | Maximum one channel may be active | Quarantine |
| No active channel | Valid; map to `other` | Keep |
| n_tokens_content = 0 | Allowed | Keep and monitor |
| Extreme shares | Allowed | Keep |

## Quarantine

Invalid records are excluded from Silver but retained with:
- original record
- batch_id
- validation error
- ingestion timestamp

This allows failed records to be investigated and reprocessed.

## Reconciliation

For each batch:

Bronze record count = Silver record count + Quarantine record count