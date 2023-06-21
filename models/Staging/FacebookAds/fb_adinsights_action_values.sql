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

with fbadinsights as (
{% set table_name_query %}
{{set_table_name('%facebook%adinsights')}}    
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
    From (
        select
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then account_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            account_currency as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        account_currency,
        account_id,
        account_name,
        actions,
        {% if target.type =='snowflake' %}
        coalesce(ACTION_VALUES.VALUE:action_type::VARCHAR,'') as action_values_action_type,
        ACTION_VALUES.VALUE:value::VARCHAR as action_values_value,
        {% else %}
        action_values.action_type as action_values_action_type,
        action_values.value as action_values_value,
        {% endif %}
        coalesce(ad_id,'') as ad_id,
        ad_name,
        adset_id,
        adset_name,
        campaign_id,
        campaign_name,
        clicks,
        CAST(date_start as DATE) date_start,
        impressions,
        spend,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,action_values.VALUE:action_type order by {{daton_batch_runtime()}} desc) row_num
        {% else %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,action_values.action_type order by {{daton_batch_runtime()}} desc) row_num
        {% endif %}
        from {{i}}
            {{unnesting("ACTION_VALUES")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    
        ) a
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(date_start) = c.date and account_currency = c.to_currency_code
        {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select * {{exclude()}}(row_num)
from fbadinsights 
where row_num =1

