-- Analysis 1: News popularity by channel

SELECT
    c.channel_name,
    COUNT(*) AS total_articles,
    SUM(f.shares) AS total_shares,
    ROUND(AVG(f.shares), 2) AS avg_shares
FROM gold.fact_news_popularity f
JOIN gold.dim_channel c
    ON f.channel_id = c.channel_id
GROUP BY c.channel_name
ORDER BY avg_shares DESC;

-- Analysis 2: News popularity by sentiment

SELECT
    s.sentiment_name,
    COUNT(*) AS total_articles,
    SUM(f.shares) AS total_shares,
    ROUND(AVG(f.shares), 2) AS avg_shares,
    ROUND(
        100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (),
        2
    ) AS article_percentage
FROM gold.fact_news_popularity f
JOIN gold.dim_sentiment s
    ON f.sentiment_id = s.sentiment_id
GROUP BY s.sentiment_name
ORDER BY avg_shares DESC;

-- Analysis 3: News popularity by weekday

SELECT
    w.weekday_name,
    COUNT(*) AS total_articles,
    SUM(f.shares) AS total_shares,
    ROUND(AVG(f.shares), 2) AS avg_shares
FROM gold.fact_news_popularity f
JOIN gold.dim_weekday w
    ON f.weekday_id = w.weekday_id
GROUP BY
    w.weekday_id,
    w.weekday_name
ORDER BY avg_shares DESC;

-- Analysis 4: Weekend vs Weekday performance

SELECT
    CASE
        WHEN w.weekday_name IN ('Saturday', 'Sunday')
            THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,

    COUNT(*) AS total_articles,

    SUM(f.shares) AS total_shares,

    ROUND(AVG(f.shares), 2) AS avg_shares,

    ROUND(
        PERCENTILE_CONT(0.5)
        WITHIN GROUP (ORDER BY f.shares)::NUMERIC,
        2
    ) AS median_shares

FROM gold.fact_news_popularity f

JOIN gold.dim_weekday w
    ON f.weekday_id = w.weekday_id

GROUP BY
    CASE
        WHEN w.weekday_name IN ('Saturday', 'Sunday')
            THEN 'Weekend'
        ELSE 'Weekday'
    END

ORDER BY avg_shares DESC;

-- Analysis 5: Gold reconciliation

SELECT
    COUNT(*) AS total_articles,
    COUNT(DISTINCT f.url) AS unique_articles,
    SUM(f.shares) AS total_shares,
    COUNT(c.channel_id) AS matched_channels,
    COUNT(s.sentiment_id) AS matched_sentiments,
    COUNT(w.weekday_id) AS matched_weekdays
FROM gold.fact_news_popularity f
LEFT JOIN gold.dim_channel c
    ON f.channel_id = c.channel_id
LEFT JOIN gold.dim_sentiment s
    ON f.sentiment_id = s.sentiment_id
LEFT JOIN gold.dim_weekday w
    ON f.weekday_id = w.weekday_id;