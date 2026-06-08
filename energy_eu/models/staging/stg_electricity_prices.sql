{{
    config(
        materialized = 'view'
    )
}}

/*
Staging model for Eurostat electricity prices.

Filters applied
---------------
  currency = 'EUR'   — drop PPS and NAC variants; keep a single unit of measure
  tax      = 'I_TAX' — all taxes and levies included; the consumer-facing price

Transformations
---------------
  geo              → country_code  (ISO alpha-2 code)
  time             → period        (Eurostat bi-annual label, e.g. '2020-S1')
  value_eur_per_kwh → price_eur_per_kwh  cast to DECIMAL(10,4)
  price_category   — derived tier: High / Medium / Low
*/

with source as (

    select * from {{ source('raw', 'raw_electricity_prices') }}

),

filtered as (

    select *
    from source
    where currency = 'EUR'
      and tax      = 'I_TAX'

),

renamed as (

    select
        -- Consumer and geography identifiers
        consumer_type,
        geo             as country_code,
        country_name,

        -- Time dimension
        time            as period,

        -- Consumption band (kept for downstream slicing by band)
        nrg_cons,
        nrg_cons_label,

        -- Tax context (constant after filter, kept for traceability)
        tax_label,

        -- Price — cast to fixed-precision decimal; DOUBLE is fine for storage
        -- but DECIMAL avoids floating-point surprises in downstream aggregations
        cast(value_eur_per_kwh as decimal(10, 4)) as price_eur_per_kwh,

        -- Derived price tier using the thresholds:
        --   High   > 0.25 EUR/kWh
        --   Medium 0.15 – 0.25 EUR/kWh  (inclusive on both ends)
        --   Low    < 0.15 EUR/kWh
        case
            when value_eur_per_kwh >  0.25 then 'High'
            when value_eur_per_kwh >= 0.15 then 'Medium'
            else                                'Low'
        end             as price_category,

        -- Lineage: when this row entered the pipeline
        loaded_at

    from filtered

)

select * from renamed
