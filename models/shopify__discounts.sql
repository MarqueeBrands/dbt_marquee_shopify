with discount as (

    select *
    from {{ var('shopify_discount_code') }}
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