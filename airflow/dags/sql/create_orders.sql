CREATE TABLE IF NOT EXISTS bebenyam5327_staging.orders (
    order_id BIGINT NOT NULL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    product_id BIGINT NOT NULL,
    unit_price BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    amount BIGINT NOT NULL
);