truncate_mrr_stocks='truncate table public.mrr_stocks;'

load_dim_stocks="""
insert into public.dim_stocks (date,company_name,symbol,open,close)
select date, company_name, symbol, open, close from mrr_stocks
on conflict on constraint dim_stocks_pk
do update set close = EXCLUDED.close;
"""

create_view_dim_stocks="""
create or replace view public.vw_dim_stocks as 
select date, company_name, symbol, open, close, cast(((close-prev_close)/prev_close)*100 as decimal(13,2)) as last_day_change_percentage from
(
select date, company_name, symbol, open, coalesce(lead(close) over(partition by symbol order by date desc), close) as prev_close, close from mrr_stocks
) v;
"""

select_from_vw_statement='select * from vw_dim_stocks where date=current_date-2 and last_day_change_percentage>=10;'