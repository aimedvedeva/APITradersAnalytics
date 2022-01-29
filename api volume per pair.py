import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date, timedelta
import datetime
import time

conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")

qwr="""
--- API volume per pair
select sum(order_inf.cost) as volume,
       order_inf.__create_date as day,
       quote.tag as quote_tag,
       base.tag as base_tag 
from view_market_aggregator_order order_inf
join view_asset_manager_currency as quote
    on order_inf.quote = quote.id
join view_asset_manager_currency as base
    on order_inf.currency = base.id
where order_inf.__create_date >= '2021-04-26'
and order_inf.client not in ('web', 'mobile', 'terminal')
and order_inf.status != 'CANCELLED'
group by day, quote.tag, base.tag; --- ~10s
"""
df=sqlio.read_sql_query(qwr, conn)

df.to_excel('4.xlsx')
#df = pd.read_excel('4.xlsx')
df['Pair'] = df['base_tag']+df['quote_tag']
quote_tags = df['quote_tag'].unique()

from datetime import datetime 
start_date = datetime(2021, 4, 26).date()
days = 14
end_date = start_date + timedelta(days=days)
import exchange
rates_dict = exchange.get_rates(quote_tags, start_date, end_date)
df.rename(columns={'day':'date'}, inplace=True)
df = exchange.quick_convert(rates_dict, df, columns=['volume'])
df = df.drop(columns=['quote_tag', 'base_tag'])
df = df.groupby(['date', 'Pair']).agg({'volume':'sum'})
df.reset_index(level=0, inplace=True)
df.reset_index(level=0, inplace=True)

import pygsheets
sheett_name = 'api volume per pair'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheett_name)

#select the first sheet
worksheet = sheet[0]
worksheet.clear()
worksheet.set_dataframe(df, (1,1), fit=True)
print('finish, babe') 

