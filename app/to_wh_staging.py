import pandas as pd
import os
from app.db_config import ENGINE


def orders_to_staging(dir: str="data"):
    df = pd.read_csv(os.path.join(dir, "orders.csv"))
    df = df.rename(columns={"total_price": "amount"})
    print(df.shape)
    df.to_sql(name="orders", con=ENGINE, 
              schema="bebeto_staging", 
              if_exists='append', index=False, 
              index_label="order_id")

def reviews_to_staging(dir: str="data"):
    df = pd.read_csv(os.path.join(dir, "reviews.csv"))
    print(df.shape)
    df.to_sql(name="reviews", con=ENGINE, 
              schema="bebeto_staging", 
              if_exists='append', index=False, 
              index_label="order_id")


def shipments_to_staging(dir: str="data"):
    df = pd.read_csv(os.path.join(dir, "shipment_deliveries.csv"))
    print(df.shape)
    df.to_sql(name="shipment_deliveries", con=ENGINE, 
              schema="bebeto_staging", 
              if_exists='append', index=False, 
              index_label="order_id")

