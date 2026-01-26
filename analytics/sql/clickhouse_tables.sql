-- Create a dedicated database
CREATE DATABASE IF NOT EXISTS analytics;

-- 1) Session fact table: one row per session (output of Flink sessionization)
-- Idempotency strategy: ReplacingMergeTree with updated_at as the version column.
-- If Flink reprocesses a session and writes it again, the newest row wins during merges.
CREATE TABLE IF NOT EXISTS analytics.fact_user_sessions
(
    session_id String,
    user_id String,

    session_start_time DateTime64(3, 'UTC'),
    session_end_time   DateTime64(3, 'UTC'),
    session_date       Date DEFAULT toDate(session_start_time),

    session_duration_seconds UInt32,
    events_count UInt32,
    unique_event_names_count UInt16,
    had_error UInt8,

    platform    LowCardinality(String),
    app_version LowCardinality(String),
    environment LowCardinality(String),

    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(session_date)
ORDER BY (session_date, user_id, session_start_time, session_id)
SETTINGS index_granularity = 8192;

-- Optional retention (uncomment if you want automatic cleanup)
-- ALTER TABLE analytics.fact_user_sessions
--   MODIFY TTL session_start_time + INTERVAL 180 DAY;


-- 2) Minute metrics aggregate: one row per minute window per dimension
-- Grain: (window_start_minute, platform, app_version, environment)
-- Idempotency strategy: ReplacingMergeTree with updated_at version column.
CREATE TABLE IF NOT EXISTS analytics.agg_metrics_minute
(
    window_start_minute DateTime64(0, 'UTC'),
    window_date         Date DEFAULT toDate(window_start_minute),

    platform    LowCardinality(String),
    app_version LowCardinality(String),
    environment LowCardinality(String),

    active_users UInt64,
    sessions_started UInt64,
    sessions_completed UInt64,
    events_total UInt64,
    error_events UInt64,

    -- Stored as Float64 to avoid integer division issues
    error_rate Float64,

    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_date)
ORDER BY (window_date, window_start_minute, platform, app_version, environment)
SETTINGS index_granularity = 8192;

-- Optional retention (uncomment if you want automatic cleanup)
-- ALTER TABLE analytics.agg_metrics_minute
--   MODIFY TTL window_start_minute + INTERVAL 30 DAY;
