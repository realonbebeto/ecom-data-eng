CREATE TABLE IF NOT EXISTS bebenyam5327_staging.shipment_deliveries (
    shipment_id BIGINT NOT NULL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    shipment_date DATE NULL,
    delivery_date DATE NULL
);