
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

with unnested_refunds as(
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
        a.closed_at,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) updated_at,
        number,
        a.note,
        total_price,
        subtotal_price,
        a.total_tax,
        taxes_included,
        a.currency,
        financial_status,
        total_discounts,
        total_line_items_price,
        a.name,
        total_price_usd,
        a.user_id,
        order_number,
        a.fulfillment_status,
        a.tags,
        contact_email,
        refunds.id as refunds_id,
        refunds.order_id as refunds_order_id,
        CAST(refunds.created_at as timestamp) refunds_created_at,
        refunds.user_id as refunds_user_id,
        CAST(refunds.processed_at as timestamp) refunds_processed_at,
        COALESCE(CAST(refund_line_items.id as string),'') as refund_line_items_id,
        refund_line_items.quantity as refund_line_items_quantity,
        refund_line_items.line_item_id as refund_line_items_line_item_id,
        refund_line_items.subtotal,
        refund_line_items.total_tax as refund_line_items_total_tax,
        line_item.id as line_item_id,
        line_item.variant_id,
        line_item.title,
        line_item.quantity,
        line_item.price,
        line_item.sku,
        line_item.variant_title,
        line_item.vendor,
        line_item.product_id,
        line_item.name as line_item_name,
        line_item.total_discount,
        line_item.fulfillment_status as line_item_fulfillment_status,
        line_item.tax_lines as line_item_tax_lines,
        refunds.transactions as refunds_transactions,
        {% if var('currency_conversion_flag') %}
            case when d.value is null then 1 else d.value end as exchange_currency_rate,
            case when d.from_currency_code is null then a.currency else d.from_currency_code end as exchange_currency_code, 
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} d on date(a.created_at) = d.date and a.currency = d.to_currency_code
            {% endif %}
            {{unnesting("refunds")}}
            {{multi_unnesting("refunds","refund_line_items")}}
            {{multi_unnesting("refund_line_items","line_item")}}
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
DENSE_RANK() OVER (PARTITION BY order_id, refund_line_items_id, variant_id order by _daton_batch_runtime desc) row_num
from unnested_refunds 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
