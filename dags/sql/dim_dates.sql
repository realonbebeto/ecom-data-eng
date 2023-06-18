CREATE TABLE IF NOT EXISTS if_common.dim_dates (
    calendar_dt DATE NOT NULL PRIMARY KEY,
    year_num BIGINT NOT NULL,
    month_of_the_year_num BIGINT NOT NULL,
    day_of_the_month_num BIGINT NOT NULL,
    day_of_the_week_num BIGINT NOT NULL,
    working_day BOOLEAN NOT NULL
);