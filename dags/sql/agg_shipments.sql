CREATE TABLE IF NOT EXISTS bebeto_analytics.agg_shipments (
    ingestion_date DATE NOT NULL PRIMARY KEY,
    tt_late_shipments BIGINT NOT NULL,
    tt_undelivered_items BIGINT NOT NULL
);