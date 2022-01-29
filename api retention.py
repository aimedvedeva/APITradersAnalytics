# =============================================================================
# -- API traders' cohort retention
# -- (API traders registered in a particular month on y-axis,
# -- how many from the registered cohort made a trade on each
# -- month since on x-axis) (filter for the daily, weekly and
# -- monthly traders - that is those who traded every day,
# -- every week, every month, other)
# --- ???
# --- extract those who've registered in April
# =============================================================================

"""
select order_inf.trader
into temp table traders
from view_market_aggregator_order order_inf
join view_user_manager_user user_inf
    on user_inf.id = order_inf.trader
where user_inf.__create_date >= '2021-04-01'
and user_inf.__create_date <= '2021-04-14'
and order_inf.client not in ('web', 'mobile', 'terminal')
and order_inf.status not in ('CANCELLED', 'REJECTED')
group by order_inf.trader;
"""

"""
select *
from view_market_aggregator_trade trade
where trade.maker_trader in
    (select * from traders)

select trader,
       count(order_info.id) as trades,
       __create_date as date
from view_market_aggregator_order order_info
where order_info.client not in ('web', 'mobile', 'terminal')
and __create_datetime >= '2021-04-01'
group by trader, date
order by date; --~3m
"""