CREATE TABLE IF NOT EXISTS bebenyam5327_analytics.agg_public_holiday (
    ingestion_date DATE NOT NULL PRIMARY KEY,
    tt_order_hol_jan BIGINT NOT NULL,
    tt_order_hol_feb BIGINT NOT NULL,
    tt_order_hol_mar BIGINT NOT NULL,
    tt_order_hol_apr BIGINT NOT NULL,
    tt_order_hol_may BIGINT NOT NULL,
    tt_order_hol_jun BIGINT NOT NULL,
    tt_order_hol_jul BIGINT NOT NULL,
    tt_order_hol_aug BIGINT NOT NULL,
    tt_order_hol_sep BIGINT NOT NULL,
    tt_order_hol_oct BIGINT NOT NULL,
    tt_order_hol_nov BIGINT NOT NULL,
    tt_order_hol_dec BIGINT NOT NULL
);