{{
    config(
        materialized='table' if target.type in ('bigquery', 'databricks', 'spark') else 'incremental',
        unique_key='discounts_unique_key',
        incremental_strategy='delete+insert' if target.type in ('postgres', 'redshift', 'snowflake') else 'merge',
        cluster_by=['discount_code_id']
        ) 
}}

with discount as (

    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['source_relation', 'discount_code_id']) }} as discounts_unique_key
    from {{ var('shopify_discount_code') }}

    {% if is_incremental() %}
    where cast(coalesce(updated_at, created_at) as date) >= {{ shopify.shopify_lookback(
        from_date="max(cast(coalesce(updated_at, created_at) as date))", 
        interval=var('lookback_window', 7), 
        datepart='day') }}
    {% endif %}
),

price_rule as (

    select * 
    from {{ ref('tmp_shopify__discount_code') }}
),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('tmp_shopify__discount_code')),
                staging_columns=get_discount_code_columns()
            )
        }}

        {{ fivetran_utils.source_relation(
            union_schema_variable='shopify_union_schemas', 
            union_database_variable='shopify_union_databases') 
        }}

    from base
),

final as (

    select 
        id as discount_code_id,
        upper(code) as code,
        price_rule_id,
        usage_count,
        {{ dbt_date.convert_timezone(column='cast(created_at as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as created_at,
        {{ dbt_date.convert_timezone(column='cast(updated_at as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as updated_at,
        {{ dbt_date.convert_timezone(column='cast(_fivetran_synced as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as _fivetran_synced,
        source_relation

    from fields
)

select *
from final