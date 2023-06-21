{% snapshot dim_product_history %}

{{
    config(
      target_schema='edm_modelling_snapshot',
      unique_key='product_key',
      strategy='check',
      check_cols='all',
    )
}}


{% set table_name_query %}
{{set_table_name_modelling('dim_product_%')}}
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
        {{ dbt_utils.surrogate_key(['product_id','sku','platform_name']) }} AS product_key,
        platform_name,
        product_name,
        product_id,
        sku,
        color,
        seller,
        size,
        description, 
        category, 
        sub_category, 
        mrp, 
        cogs, 
        start_date, 
        end_date,
        row_number() over(partition by product_id,sku,platform_name, start_date, end_date order by {{daton_batch_runtime()}} desc) row_num
	      from {{i}}) where row_num = 1
        
    {% if not loop.last %} union all {% endif %}
        
{% endfor %}

{% endsnapshot %}