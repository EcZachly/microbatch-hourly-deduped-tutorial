CREATE TABLE deduped_source (
    product_id BIGINT,
    event_type BIGINT,
    event_locations MAP<STRING, BIGINT>,
    min_event_timestamp_epoch BIGINT,
    max_event_timestamp_epoch BIGINT
)
PARTITIONED BY(ds STRING, product_name STRING)