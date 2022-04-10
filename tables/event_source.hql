-- The source of our events
-- product_id

CREATE TABLE event_source (
    product_id BIGINT,
    event_type STRING,
    event_timestamp_epoch BIGINT,
    event_location STRING
)
PARTITIONED BY(ds STRING, hour STRING, product_name STRING)