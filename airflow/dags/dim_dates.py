"""
I had built this script to simulate the dim_dates
"""


import pandas as pd
import holidays
from datetime import timedelta, datetime
from db_config import return_engine

ENGINE = return_engine("main_db")

# Used to choose Holland holidays
nl_holidays = holidays.country_holidays('NL')


def dimension_dates(start_date: str = "2021-01-01", end_date: str ="2022-09-06"):
    start = datetime.strptime(f"{start_date}", '%Y-%m-%d').date()
    end  = datetime.strptime(f"{end_date}", '%Y-%m-%d').date()
    diff = (end - start).days
    
    dates = []
    for i in range(diff):
        # create dates
        date = start + timedelta(i)
        dates.append(date)
    
    # create dataframe with dates columnn
    df = pd.DataFrame({"calendar_dt": dates})

    # create year column
    df['year_num'] = df.calendar_dt.apply(lambda x: x.year)

    # create month column
    df['month_of_the_year_num'] = df.calendar_dt.apply(lambda x: x.month)

    # create month day column
    df['day_of_the_month_num'] = df.calendar_dt.apply(lambda x: x.day)

    # create week day column
    df['day_of_the_week_num'] = df.calendar_dt.apply(lambda x: x.weekday())

    # create working day column using the holiday package
    df['working_day'] = df.calendar_dt.apply(lambda x: x in nl_holidays)

    # df.to_csv("./data/dates.csv", index=False)

    df.to_sql(name="dim_dates", con=ENGINE, schema="if_common", if_exists='append', index=False, index_label="calendar_dt")
