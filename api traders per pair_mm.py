import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date, timedelta
import datetime
import time

conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")

# =============================================================================
# --- API traders per pair (mm)
# =============================================================================

qwr="""
begin;
Create temp table trader on commit drop
as
select trader
        from view_market_aggregator_order
        where __create_date >= '2021-04-01'
        and client not in ('web', 'mobile', 'terminal')
        and status not in ('CANCELLED', 'REJECTED')
        group by trader;
select count(distinct trade.maker_trader) as traders, -- taker = maker
       date_trunc('day', trade.__create_datetime) as day,
       quote.tag,
       base.tag
from view_market_aggregator_trade as trade
join view_asset_manager_currency as quote
    on trade.quote = quote.id
join view_asset_manager_currency as base
    on trade.currency = base.id
join trader t
    on t.trader=trade.maker_trader
where
 trade.maker_trader = trade.taker_trader
and trade.__create_date >= DATE(%s)
and trade.__create_date < DATE(%s)
group by day, quote.tag, base.tag;
drop table trader
--commit;
"""

#df=sqlio.read_sql_query(qwr, conn)
#df.to_excel('555.xlsx')

start_date = datetime.datetime(2021, 4, 26)
days = 14
end_date = start_date + timedelta(days=days)

def day_select(start_date, end_date, query, conn):
    data = pd.DataFrame()
    while start_date < end_date:
        
        next_date = start_date + timedelta(days=1) 
       
        conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")

        day_data = sqlio.read_sql_query(query, conn, params=(start_date, next_date))
        
        day_data['date'] = start_date

        data = data.append(day_data)
                    
        start_date = start_date + timedelta(days=1)
        print('next day')
    return data

#df = day_select(start_date, end_date, qwr, conn)
#df.to_excel('3.xlsx')