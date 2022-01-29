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
# --- API traders per pair
# =============================================================================
qwr="""
select count(distinct order_inf.trader) filter (where order_inf.status != 'CANCELLED' and
                                             order_inf.status != 'REJECTED') as traders,
       date_trunc('day', order_inf.__create_datetime) as day,
       quote.tag,
       base.tag
from view_market_aggregator_order order_inf
join view_asset_manager_currency as quote
    on order_inf.quote = quote.id
join view_asset_manager_currency as base
    on order_inf.currency = base.id

where order_inf.client not in ('web', 'mobile', 'terminal')
and order_inf.__create_datetime >= '2021-04-26'
group by day, quote.tag, base.tag
order by day; 
"""
df=sqlio.read_sql_query(qwr, conn)
df.to_excel('2.xlsx')
#df = pd.read_excel('2.xlsx')
df['Pair'] = df['tag.1']+df['tag']
df = df.drop(columns=['tag.1', 'tag'])
df = df.groupby(['day', 'Pair']).agg({'traders':'sum'})
df.reset_index(level=0, inplace=True)
df.reset_index(level=0, inplace=True)

import pygsheets
sheett_name = 'api traders per pair'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheett_name)

#select the first sheet
worksheet = sheet[0]
worksheet.clear()
worksheet.set_dataframe(df, (1,1), fit=True)
print('finish, babe') 
