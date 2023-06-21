
{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

with unnested_customers as(
{% set table_name_query %}
{{set_table_name('%weezie%shopify%orders%')}}    
{% endset %}  


{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * 
    FROM (
        select 
        cast(a.id as string) order_id, 
        a.email,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) updated_at,
        number,
        a.note,
        total_price,
        subtotal_price,
        total_tax,
        taxes_included,
        currency,
        financial_status,
        total_discounts,
        total_line_items_price,
        a.name,
        total_price_usd,
        user_id,
        order_number,
        fulfillment_status,
        a.tags,
        contact_email,
        BILLING_ADDRESS.first_name as billing_address_first_name,
        BILLING_ADDRESS.address1 as billing_address_address1,
        BILLING_ADDRESS.phone as billing_address_phone,
        BILLING_ADDRESS.city as billing_address_city,
        BILLING_ADDRESS.zip as billing_address_zip,
        BILLING_ADDRESS.province as billing_address_province,
        BILLING_ADDRESS.country as billing_address_country,
        BILLING_ADDRESS.last_name as billing_address_last_name,
        BILLING_ADDRESS.address2 as billing_address_address2,
        BILLING_ADDRESS.company as billing_address_company,
        BILLING_ADDRESS.latitude as billing_address_latitude,
        BILLING_ADDRESS.longitude as billing_address_longitude,
        BILLING_ADDRESS.name as billing_address_name,
        BILLING_ADDRESS.country_code as billing_address_country_code,
        BILLING_ADDRESS.province_code as billing_address_province_code,
        SHIPPING_ADDRESS.first_name as shipping_address_first_name,
        SHIPPING_ADDRESS.address1 as shipping_address_address1,
        SHIPPING_ADDRESS.phone as shipping_address_phone,
        SHIPPING_ADDRESS.city as shipping_address_city,
        SHIPPING_ADDRESS.zip as shipping_address_zip,
        SHIPPING_ADDRESS.province as shipping_address_province,
        SHIPPING_ADDRESS.country as shipping_address_country,
        SHIPPING_ADDRESS.last_name as shipping_address_last_name,
        SHIPPING_ADDRESS.address2 as shipping_address_address2,
        SHIPPING_ADDRESS.company as shipping_address_company,
        SHIPPING_ADDRESS.latitude as shipping_address_latitude,
        SHIPPING_ADDRESS.longitude as shipping_address_longitude,
        SHIPPING_ADDRESS.name as shipping_address_name,
        SHIPPING_ADDRESS.country_code as shipping_address_country_code,
        SHIPPING_ADDRESS.province_code as shipping_address_province_code,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("BILLING_ADDRESS")}}
            {{unnesting("SHIPPING_ADDRESS")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}


),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY order_id order by _daton_batch_runtime desc) row_num
from unnested_customers 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
