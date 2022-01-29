import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date, timedelta
import datetime
import time

conn = psycopg2.connect("dbname='postgres' user='' host='' password=''")

# =============================================================================
# -- API traders' submitted orders
# -- (histogram with x-axis orders amount range
# -- (0-200, 200-500, 500-1,000, 1,000-20,000, >20,000)
# -- and y-axis the number of traders for this range)
# -- (filter for months, so we can compare this histogram by months)
# -- 2021
#-- API users' orders split into (cancelled, filled, placed for each day)
# =============================================================================

qwr="""
select count(order_info.id) filter (where status = 'CANCELLED') as cancelled,
       count(order_info.id) filter (where status = 'PLACED') as placed,
       count(order_info.id) filter (where status = 'REJECTED') as rejected,
       count(order_info.id) filter (where status = 'CLOSED') as closed,
       count(order_info.id) as total,
       date_trunc('day', __create_datetime) as day
from view_market_aggregator_order order_info
where order_info.client not in ('web', 'mobile', 'terminal')
and __create_datetime >= '2021-04-01'
group by day
order by day; ---~3m 36s
"""

df=sqlio.read_sql_query(qwr, conn)
#set connection
import pygsheets
sheett_name = 'traders types'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheett_name)

#select the first sheet
worksheet = sheet[0]
worksheet.clear()
worksheet.set_dataframe(df, (1,1), fit=True)
print('finish, babe')

# =============================================================================
# select trader,
#        count(order_info.id) filter (where status != 'CANCELLED' and
#                                           status != 'REJECTED') as submitted_orders,
#        date_trunc('month', __create_datetime) as month
# from view_market_aggregator_order order_info
# where order_info.client not in ('web', 'mobile', 'terminal')
# and __create_datetime >= '2021-04-01'
# group by trader, month
# order by month; --~3m
# =============================================================================
