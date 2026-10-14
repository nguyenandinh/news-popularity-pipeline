INSERT INTO gold.dim_channel (channel_name)
VALUES
    ('Lifestyle'),
    ('Entertainment'),
    ('Business'),
    ('Social Media'),
    ('Technology'),
    ('World'),
    ('Unknown')
ON CONFLICT (channel_name) DO NOTHING;

INSERT INTO gold.dim_sentiment (sentiment_name)
VALUES
    ('Positive'),
    ('Neutral'),
    ('Negative')
ON CONFLICT (sentiment_name) DO NOTHING;

INSERT INTO gold.dim_weekday (weekday_id, weekday_name)
VALUES
    (1, 'Monday'),
    (2, 'Tuesday'),
    (3, 'Wednesday'),
    (4, 'Thursday'),
    (5, 'Friday'),
    (6, 'Saturday'),
    (7, 'Sunday')
ON CONFLICT (weekday_id) DO NOTHING;