with customer_addresses as (
    (
    SELECT email,
lower(trim(billing_address_first_name)) as first_name,
lower(trim(billing_address_last_name)) as last_name,
lower(trim(billing_address_address1)) as address1,
lower(trim(billing_address_address2)) as address2,
lower(trim(billing_address_city)) as city,
lower(trim(billing_address_province)) as state,
billing_address_zip as zip,'Billing' as source,
billing_address_country as country
FROM {{ ref('spy_orders_addresses') }})

union all

(select email,
lower(trim(shipping_address_first_name)),
lower(trim(shipping_address_last_name)),
lower(trim(shipping_address_address1)),
lower(trim(shipping_address_address2)),
lower(trim(shipping_address_city)),
lower(trim(shipping_address_province)),
shipping_address_zip,'Shipping' as source,
shipping_address_country as country

FROM {{ ref('spy_orders_addresses') }}
where billing_address_name<>shipping_address_name)),

addresses as (select distinct *
from customer_addresses 
where country='United States')
select 
REGEXP_REPLACE(INITCAP(first_name),r'\s+', ' ') as firstName,
REGEXP_REPLACE(INITCAP(last_name),r'\s+', ' ')  as lastName,
REGEXP_REPLACE(INITCAP(address1),r'\s+', ' ') as Address1,
case when lower(Address2)='null' then '' else REGEXP_REPLACE(INITCAP(address2),r'\s+', ' ') end as Address2,
INITCAP(city) as City,INITCAP(state) as State,Zip,date(b.last_order_date) as lastOrderDate
from addresses a 
left join (
    select distinct email,last_order_date from {{ ref('dim_customer') }}
) b on a.email=b.email