{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['date','order_key'],
    unique_key = ['order_key','platform_key','product_key','date','amount_type','transaction_type','charge_type'])}}

    
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
{{set_table_name_modelling('fact_finances%')}}
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

        Select 
        {{ dbt_utils.surrogate_key(['order_id']) }} AS order_key,
        {{ dbt_utils.surrogate_key(['platform_name']) }} AS platform_key,
        {{ dbt_utils.surrogate_key(['product_id', 'sku', 'platform_name']) }} AS product_key,
        date,
        amount_type,
        transaction_type,
        charge_type,
        amount,
        exchange_currency_code as currency_code,
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
