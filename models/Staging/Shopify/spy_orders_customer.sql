
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
{{set_table_name('%test%rawdata%sud%')}}    
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
        CUSTOMER.id as customer_id,
        CUSTOMER.email as customer_email,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(CUSTOMER.created_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as customer_created_at,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(CUSTOMER.updated_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as customer_updated_at,
        CUSTOMER.first_name as customer_first_name,
        CUSTOMER.last_name as customer_last_name,
        CUSTOMER.orders_count,
        CUSTOMER.state,
        CUSTOMER.total_spent,
        CUSTOMER.last_order_id,
        CUSTOMER.phone as customer_phone,
        CUSTOMER.tags as customer_tags,
        CUSTOMER.last_order_name,
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
            {{unnesting("CUSTOMER")}}
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
DENSE_RANK() OVER (PARTITION BY order_id, customer_id order by _daton_batch_runtime desc) row_num
from unnested_customers 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
