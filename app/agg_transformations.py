from app.db_config import ENGINE
import pandas as pd

SCHEMA = "bebeto_analytics"


def agg_public_holiday():

    query = """
        SELECT CURRENT_DATE AS ingestion_date,
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 1 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_jan",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 2 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_feb",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 3 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_mar",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 4 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_apr",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 5 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_may",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 6 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_jun",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 7 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_jul",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 8 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_aug",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 9 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_sep",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 10 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_oct",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 11 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_nov",
                       SUM(CASE
                               WHEN v.month_of_the_year_num = 12 THEN v.order_count
                               ELSE 0
                           END) AS "tt_order_hol_dec"
        FROM
        ( SELECT dt.month_of_the_year_num,
                COUNT(o.order_id) AS order_count
        FROM bebeto_staging."orders" o
        INNER JOIN if_common."dim_dates" dt ON dt.calendar_dt = o.order_date
        WHERE dt.day_of_the_week_num IN (1,
                                            2,
                                            3,
                                            4,
                                            5)
            AND dt.working_day = TRUE
            AND DATE_TRUNC('year', o.order_date) = DATE_TRUNC('year', date '2022-09-05') - INTERVAL '1 year'
        GROUP BY dt.month_of_the_year_num
        ORDER BY 1 ASC ) v
        GROUP BY 1;
    """

    part_data = ENGINE.execute(query)
    read_data = part_data.fetchall()
    col_names = part_data.keys()
    df = pd.DataFrame(read_data, columns=col_names)
    df.to_sql(name="agg_public_holiday", schema=SCHEMA, con=ENGINE, if_exists="append", index_label="ingestion_date")


def agg_shipments():
    
    
    query = """
        SELECT CURRENT_DATE AS ingestion_date,
                       COUNT(CASE
                                 WHEN sd.shipment_date >= o.order_date + INTERVAL '6 days'
                                      AND sd.delivery_date IS NULL THEN 1
                             END) AS tt_late_shipments,
                       COUNT(CASE
                                 WHEN sd.delivery_date IS NULL
                                      AND sd.shipment_date IS NULL
                                      AND CURRENT_DATE >= o.order_date + INTERVAL '15 days' THEN 1
                             END) AS tt_undelivered_items
        FROM bebeto_staging.shipment_deliveries sd
        LEFT JOIN bebeto_staging.orders o ON o.order_id=sd.order_id;
    """

    part_data = ENGINE.execute(query)
    read_data = part_data.fetchall()
    col_names = part_data.keys()
    df = pd.DataFrame(read_data, columns=col_names)
    df.to_sql(name="agg_shipments", schema=SCHEMA, con=ENGINE, if_exists="append", index_label="ingestion_date")


def best_performing_product():
    
    query = """
    WITH highest_reviewed_product AS
        (SELECT r.product_id,
                COUNT(r.review) AS total_reviews,
                SUM(r.review) AS total_review_points
        FROM bebeto_staging.reviews r
        GROUP BY 1
        ORDER BY total_reviews DESC
        LIMIT 1),
            most_ordered_date AS
        (SELECT o.order_date,
                COUNT(o.order_id) AS total_orders
        FROM bebeto_staging.orders o
        WHERE o.product_id =
            (SELECT product_id
                FROM highest_reviewed_product)
        GROUP BY o.order_date
        ORDER BY total_orders DESC
        LIMIT 1),
            public_holiday AS
        (SELECT working_day
        FROM "if_common.dim_dates"
        WHERE calendar_dt IN
            (SELECT order_date
                FROM most_ordered_date)
        LIMIT 1),
            review_points_distribution AS
        (SELECT review,
                COUNT(*) AS COUNT,
                COUNT(*) * 100.0 /
            (SELECT COUNT(*)
            FROM bebeto_staging.reviews
            WHERE product_id =
                (SELECT product_id
                FROM highest_reviewed_product) ) AS percentage
        FROM bebeto_staging.reviews
        WHERE product_id =
            (SELECT product_id
                FROM highest_reviewed_product)
        GROUP BY review
        ORDER BY review),
            shipment_distribution AS
        (SELECT CASE
                    WHEN sd.shipment_date >= (o.order_date + interval '6 days')
                        AND sd.delivery_date IS NULL THEN 'Late Shipment'
                    ELSE 'Early Shipment'
                END AS shipment_type,
                COUNT(*) AS COUNT,
                COUNT(*) * 100.0 /
            (SELECT COUNT(*)
            FROM bebeto_staging.shipment_deliveries
            WHERE order_id IN
                (SELECT order_id
                FROM bebeto_staging.orders
                WHERE product_id =
                    (SELECT product_id
                        FROM highest_reviewed_product) ) ) AS percentage
        FROM bebeto_staging.shipment_deliveries sd
        INNER JOIN bebeto_staging.orders o ON o.order_id=sd.order_id
        WHERE sd.order_id IN
            (SELECT order_id
                FROM bebeto_staging.orders o
                WHERE product_id =
                    (SELECT product_id
                    FROM highest_reviewed_product) )
        GROUP BY shipment_type)
    SELECT CURRENT_DATE AS ingestion_date,

        (SELECT product_id
        FROM highest_reviewed_product) AS product_id,

        (SELECT order_date
        FROM most_ordered_date) AS most_ordered_day,

        (SELECT working_day
        FROM public_holiday) AS is_public_holiday,

        (SELECT total_review_points
        FROM highest_reviewed_product) AS tt_review_points,

        (SELECT round(percentage, 2)
        FROM review_points_distribution
        WHERE review = 1) AS pct_one_star_review,

        (SELECT round(percentage, 2)
        FROM review_points_distribution
        WHERE review = 2) AS pct_two_star_review,

        (SELECT round(percentage, 2)
        FROM review_points_distribution
        WHERE review = 3) AS pct_three_star_review,

        (SELECT round(percentage, 2)
        FROM review_points_distribution
        WHERE review = 4) AS pct_four_star_review,

        (SELECT round(percentage, 2)
        FROM review_points_distribution
        WHERE review = 5) AS pct_five_star_review,

        (SELECT round(percentage, 2)
        FROM shipment_distribution
        WHERE shipment_type = 'Early Shipment') AS pct_early_shipments,

        (SELECT round(percentage, 2)
        FROM shipment_distribution
        WHERE shipment_type = 'Late Shipment') AS pct_late_shipments;
    """

    part_data = ENGINE.execute(query)
    read_data = part_data.fetchall()
    col_names = part_data.keys()
    df = pd.DataFrame(read_data, columns=col_names)
    df.to_sql(name="best_performing_product", schema=SCHEMA, con=ENGINE, if_exists="append", index_label="ingestion_date")