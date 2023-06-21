{{config( 
    materialized ='incremental', 
    incremental_strategy ='merge', 
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['date','customer_key'],
    unique_key = ['date','customer_key','platform_key','order_key','is_cancelled','transaction_type'])}}
  
    
{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX({{to_epoch_milliseconds('last_updated')}}) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name_modelling('fact_orders%')}}
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
{% endif %}

{% for i in results_list %}

        select 
        {{ dbt_utils.surrogate_key(['order_id']) }} AS order_key,
        {{ dbt_utils.surrogate_key(['platform_name']) }} AS platform_key,
        {{ dbt_utils.surrogate_key(['email']) }} AS customer_key,
        date,
        transaction_type,
        quantity,line_items,
        round((gross_amount/exchange_currency_rate),2) as gross_amount,
        round((total_tax/exchange_currency_rate),2) as total_tax,
        round((shipping_price/exchange_currency_rate),2) as shipping_price,
        round((discounts/exchange_currency_rate),2) as discounts,
        exchange_currency_code as currency_code,
        is_cancelled,
        {% if var('timezone_conversion_flag') %}
            DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
            null as run_id
        {% else %}
            current_timestamp() as last_updated,
            null as run_id
        {% endif %}
	      from {{i}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
            {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}