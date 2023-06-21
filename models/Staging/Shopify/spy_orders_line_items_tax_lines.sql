
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

    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id order by _daton_batch_runtime desc) _seq_id
    from (
    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        cast(a.id as string) order_id, 
        email,
        closed_at,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) updated_at,
        number,
        note,
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
        a.fulfillment_status,
        tags,
        contact_email,
        line_items.id as line_items_id,
        line_items.variant_id as variant_id,
        line_items.title as title,
        line_items.quantity as quantity,
        cast(line_items.price as numeric) price,
        line_items.sku as sku,
        line_items.variant_title as variant_title,
        line_items.vendor as vendor,
        line_items.product_id as product_id,
        line_items.name as line_items_name, 
        cast(line_items.total_discount as numeric) line_items_total_discount,
        line_items.fulfillment_status as line_items_fulfillment_status,
        tax_lines.title as line_items_tax_lines_title,
        tax_lines.price as line_items_tax_lines_price,
        tax_lines.rate as line_items_tax_lines_rate,
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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY a.id order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {{unnesting("line_items")}}
                {{multi_unnesting("line_items","tax_lines")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1 )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
