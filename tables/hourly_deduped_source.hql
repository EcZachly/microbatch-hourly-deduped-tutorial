CREATE TABLE hourly_deduped_source (
    product_id BIGINT,
    product_event_type BIGINT,
    min_event_timestamp_epoch BIGINT,
    max_event_timestamp_epoch BIGINT,
    event_locations MAP<STRING, BIGINT>
)
PARTITIONED BY(ds STRING, hour string, product_name STRING)