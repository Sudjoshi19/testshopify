{% snapshot dim_subscriptions_history %}

{{
    config(
      target_schema='edm_modelling_snapshot',
      unique_key='subscription_key',
      strategy='check',
      check_cols='all',
    )
}}

{% set table_name_query %}
{{set_table_name_modelling('dim_subscription%')}}
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

        select * except(row_num) from (
        select 
        distinct
        {{ dbt_utils.surrogate_key(['subscription_id','sku']) }} AS subscription_key,
        {{ dbt_utils.surrogate_key(['brand'])}} AS brand_key,
        {{ dbt_utils.surrogate_key(['sku'])}} AS product_key,
        {{ dbt_utils.surrogate_key(['platform_name','store_name'])}} AS platform_key,
        {{ dbt_utils.surrogate_key(['external_product_id','sku','platform_name'])}} AS product_platform_key,
        order_channel,
        subscription_id,
        customer_id,
        utm_source,
        utm_medium,
        created_at,
        next_charge_scheduled_at,
        order_interval_frequency,
        order_interval_unit,
        status,
        updated_at,
        cancellation_reason,
        cancelled_at,
        order_day_of_month,
        presentment_currency,
        cancellation_reason_comments,
        row_number() over(partition by external_product_id,sku,platform_name order by {{daton_batch_runtime()}} desc) row_num
	      from {{i}}) where row_num = 1
        
        {% if not loop.last %} union all {% endif %}
        
{% endfor %}

{% endsnapshot %}