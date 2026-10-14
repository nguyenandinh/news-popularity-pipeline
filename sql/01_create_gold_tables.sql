CREATE TABLE IF NOT EXISTS gold.dim_channel (
    channel_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS gold.dim_sentiment (
    sentiment_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sentiment_name VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS gold.dim_weekday (
    weekday_id INTEGER PRIMARY KEY,
    weekday_name VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS gold.fact_news_popularity (
    article_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    url TEXT NOT NULL UNIQUE,

    channel_id INTEGER NOT NULL
        REFERENCES gold.dim_channel(channel_id),

    sentiment_id INTEGER NOT NULL
        REFERENCES gold.dim_sentiment(sentiment_id),

    weekday_id INTEGER NOT NULL
        REFERENCES gold.dim_weekday(weekday_id),

    shares BIGINT NOT NULL CHECK (shares >= 0),

    batch_id VARCHAR(8) NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
);