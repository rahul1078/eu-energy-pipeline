{{
    config(
        materialized = 'table'
    )
}}

/*
Mart: Annual electricity prices by country and consumer type.

Grain: one row per (country_code, consumer_type, year).

Source: stg_electricity_prices
  - Filtered to nrg_cons = 'TOT_KWH' (the all-bands aggregate) so that
    individual consumption bands are not mixed into the averages.
  - Two periods per year (S1 / S2) are averaged into a single annual figure.

Window functions
  - prior_year_avg_price : LAG(avg_price) partitioned by country + consumer_type
  - yoy_change_pct       : (current - prior) / prior * 100, NULL for the first year
                           and guarded against division by zero

price_category_mode
  - Most common category across the S1/S2 periods in a year.
  - Ties (one period High, one Medium) are broken by favouring the higher tier:
    High > Medium > Low. Reflects the conservative "at least half the year was at
    this tier" interpretation.
*/

with stg as (

    select * from {{ ref('stg_electricity_prices') }}

),

-- Annual averages — average S1 and S2 into one row per year
yearly_agg as (

    select
        country_code,
        country_name,
        cast(left(period, 4) as integer)          as year,
        consumer_type,
        round(avg(price_eur_per_kwh), 4)          as avg_price_eur_per_kwh,
        min(price_eur_per_kwh)                    as min_price,
        max(price_eur_per_kwh)                    as max_price
    from stg
    where nrg_cons = 'TOT_KWH'
    group by 1, 2, 3, 4

),

-- Count category occurrences per year-group so we can find the mode
category_counts as (

    select
        country_code,
        cast(left(period, 4) as integer)          as year,
        consumer_type,
        price_category,
        count(*)                                  as n
    from stg
    where nrg_cons = 'TOT_KWH'
    group by 1, 2, 3, 4

),

-- Pick the most frequent category; ties resolved by tier rank (High > Medium > Low)
category_mode as (

    select distinct on (country_code, year, consumer_type)
        country_code,
        year,
        consumer_type,
        price_category                            as price_category_mode
    from category_counts
    order by
        country_code,
        year,
        consumer_type,
        n                                                                    desc,
        case price_category
            when 'High'   then 1
            when 'Medium' then 2
            else               3
        end                                                                  asc

),

-- Join mode back, then apply LAG over the ordered annual series
with_lag as (

    select
        y.country_code,
        y.country_name,
        y.year,
        y.consumer_type,
        y.avg_price_eur_per_kwh,
        y.min_price,
        y.max_price,
        m.price_category_mode,

        lag(y.avg_price_eur_per_kwh) over (
            partition by y.country_code, y.consumer_type
            order by y.year
        )                                         as prior_year_avg_price

    from yearly_agg       y
    inner join category_mode m
        on  y.country_code  = m.country_code
        and y.year          = m.year
        and y.consumer_type = m.consumer_type

)

select
    country_code,
    country_name,
    year,
    consumer_type,
    avg_price_eur_per_kwh,
    min_price,
    max_price,
    price_category_mode,
    prior_year_avg_price,

    -- YoY % change; NULL when no prior year exists or prior price is zero
    case
        when prior_year_avg_price is null
          or prior_year_avg_price = 0             then null
        else round(
            (avg_price_eur_per_kwh - prior_year_avg_price)
            / prior_year_avg_price * 100,
            2
        )
    end                                           as yoy_change_pct

from with_lag
order by country_code, consumer_type, year
