{% if var('currency_conversion_flag') %}
 --depends_on: {{ ref('ExchangeRates') }}
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

{% set table_name_query %}
{{set_table_name('%googleads%shopping_performance_view')}}    
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

    SELECT * {{exclude()}} (row_num)
    From (
        select
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.currency_code else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            cast(null as string) as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        'USD' as currency_code,
        CAMPAIGN.name as campaign_name,
        COALESCE(CAMPAIGN.id,0) as campaign_id,
        CAMPAIGN.advertising_channel_type as campaign_advertising_channel_type,
        AD_GROUP.id as ad_group_id,
        AD_GROUP.name as ad_group_name,
        METRICS.clicks,
        METRICS.conversions,
        METRICS.cost_micros,
        METRICS.conversions_value,
        METRICS.impressions,
        SEGMENTS.date,
        SEGMENTS.product_item_id as product_item_id,
        SEGMENTS.product_title as product_title,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        Dense_Rank() OVER (PARTITION BY CUSTOMER.resource_name, SEGMENTS.date, AD_GROUP.id,CAMPAIGN.id,SEGMENTS.product_item_id,SEGMENTS.product_title 
        order by {{daton_batch_runtime()}} desc) row_num
	    from {{i}} 
            {{unnesting("CUSTOMER")}}
            {{unnesting("CAMPAIGN")}}
            {{unnesting("AD_GROUP")}}
            {{unnesting("METRICS")}}
            {{unnesting("SEGMENTS")}}
            {{unnesting("SHOPPING_PERFORMANCE_VIEW")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    
        ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.date) = c.date and currency_code = c.to_currency_code  
            {% endif %}
    )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
