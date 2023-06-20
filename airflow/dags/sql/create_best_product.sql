CREATE TABLE IF NOT EXISTS bebenyam5327_analytics.best_performing_product (
    ingestion_date DATE NOT NULL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    most_ordered_day DATE NOT NULL,
    is_public_holiday BOOLEAN NOT NULL,
    tt_review_points BIGINT NOT NULL,
    pct_one_star_review DOUBLE PRECISION NOT NULL,
    pct_two_star_review DOUBLE PRECISION NOT NULL,
    pct_three_star_review DOUBLE PRECISION NOT NULL,
    pct_four_star_review DOUBLE PRECISION NOT NULL,
    pct_five_star_review DOUBLE PRECISION NOT NULL,
    pct_early_shipments DOUBLE PRECISION NOT NULL,
    pct_late_shipments DOUBLE PRECISION NOT NULL
);