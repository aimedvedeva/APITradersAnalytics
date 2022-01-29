import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date, timedelta
import datetime
import time
from func_timeout import func_timeout, FunctionTimedOut

#conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")

batch_qwr="""
select trade.maker_trader as trader, -- taker = maker
       date_trunc('day', trade.__create_datetime) as day,
       quote.tag,
       base.tag,
       nonce
from view_market_aggregator_trade as trade
join view_asset_manager_currency as quote 
    on trade.quote = quote.id
join view_asset_manager_currency as base
    on trade.currency = base.id
where trade.maker_trader in
    (   select trader
        from view_market_aggregator_order
        where __create_datetime >= '2021-04-01'
        and client not in ('web', 'mobile', 'terminal')
        and status not in ('CANCELLED', 'REJECTED')
        group by trader
    )
and trade.maker_trader = trade.taker_trader
and trade.__create_datetime >= DATE(%s)
and trade.__create_datetime < DATE(%s)
and nonce > %s
group by day, quote.tag, base.tag, trader, nonce
order by day
limit %s;
"""
day_qwr="""
select count(distinct trade.maker_trader) as traders, -- taker = maker
       date_trunc('day', trade.__create_datetime) as day,
       quote.tag,
       base.tag
from view_market_aggregator_trade as trade
join view_asset_manager_currency as quote 
    on trade.quote = quote.id
join view_asset_manager_currency as base
    on trade.currency = base.id
where trade.maker_trader in
    (   select trader
        from view_market_aggregator_order
        where __create_datetime >= '2021-04-01'
        and client not in ('web', 'mobile', 'terminal')
        and status not in ('CANCELLED', 'REJECTED')
        group by trader
    )
and trade.maker_trader = trade.taker_trader
and trade.__create_datetime >= DATE(%s)
and trade.__create_datetime < DATE(%s)
group by day, quote.tag, base.tag;
"""
day_qwr_with_tmp_tbl = """
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

def batch_select(query, batch_size, other_params):
    last_nonce = 0
    
    while True:
        batch_data = pd.DataFrame()
        
        conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")
        while(batch_data.empty):
            try:
                batch_data = func_timeout(300, \
                                        sqlio.read_sql_query, \
                                        args=(query, conn, None, True, \
                                              (*other_params, last_nonce, batch_size))\
                                       )
            except FunctionTimedOut:
                print('\nbatch selection could not complete within 300 seconds')
                batch_size = batch_size / 2
            except Exception:
                pass
                #handle other exceptions
                
        #batch_data = sqlio.read_sql_query(query, conn, None, True, (*other_params, last_nonce, batch_size))
        
        if (last_nonce == 0):
            data = batch_data
        else:
            data = data.append(batch_data)
        
        if(len(batch_data) < batch_size):
            break;

        last_nonce = batch_data['nonce'].iloc[-1].item()
        print('next batch')
    return data

def day_select(start_date, end_date, day_query, day_qwr_with_tmp_tbl):
    data = pd.DataFrame()
    while start_date < end_date:
        
        next_date = start_date + timedelta(days=1) 
       
        conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")
        
        try:
            day_data = func_timeout(60, \
                                    sqlio.read_sql_query, \
                                    args=(day_query, conn, None, True, (start_date, next_date))\
                                   )
        except FunctionTimedOut:
# =============================================================================
#             print('\nday selection could not complete within 60 seconds')
#             day_data = batch_select(batch_query, 500000, other_params=(start_date, next_date))
#             day_data = day_data.drop_duplicates(subset=['trader', 'day', 'quote', 'base'], keep='first')
#             day_data = day_data.drop(columns=['nonce'])
#             day_data = day_data.groupby(['quote', 'base', 'day']).agg({'id':'count'})
# =============================================================================
             day_data = func_timeout(200, \
                                    sqlio.read_sql_query, \
                                    args=(day_qwr_with_tmp_tbl, conn, None, True, (start_date, next_date))\
                                    )
        except Exception:
            pass
            #handle other exceptions
        
        day_data['date'] = start_date

        data = data.append(day_data)
                    
        start_date = start_date + timedelta(days=1)
        print('next day')
    return data

start_date = datetime.datetime(2021, 4, 26)
from datetime import datetime
start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0)
days = 14
end_date = start_date + timedelta(days=days)

df = day_select(start_date, end_date, day_qwr, day_qwr_with_tmp_tbl)
df.to_excel('3.xlsx')

#5000000/2^13