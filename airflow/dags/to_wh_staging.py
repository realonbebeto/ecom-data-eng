import pandas as pd
import os
from db_config import return_engine

ENGINE = return_engine("main_db")
DIR  = os.getcwd()


"""
Functions that read downloaded files and ingests them to specific tables
"""

def orders_to_staging():
    df = pd.read_csv(os.path.join(DIR, "tmp_data/orders.csv"))
    df = df.rename(columns={"total_price": "amount"})
    print(df.shape)
    df.to_sql(name="orders", con=ENGINE, 
              schema="bebenyam5327_staging", 
              if_exists='append', index=False, 
              index_label="order_id")

def reviews_to_staging():
    df = pd.read_csv(os.path.join(DIR, "tmp_data/reviews.csv"))
    print(df.shape)
    df.to_sql(name="reviews", con=ENGINE, 
              schema="bebenyam5327_staging", 
              if_exists='append', index=False, 
              index_label="order_id")


def shipments_to_staging():
    df = pd.read_csv(os.path.join(DIR, "tmp_data/shipment_deliveries.csv"))
    print(df.shape)
    df.to_sql(name="shipment_deliveries", con=ENGINE, 
              schema="bebenyam5327_staging", 
              if_exists='append', index=False, 
              index_label="order_id")

