/* 
Exercise

Virtual Kitchen has an emergency! 

We shipped several meal kits without including fresh parsley, and our customers are starting to complain. We have identified the impacted cities, and we know that 25 of our customers did not get their parsley. That number might seem small, but Virtual Kitchen is committed to providing every customer with a great experience.

Our management has decided to provide a different recipe for free (if the customer has other preferences available), or else use grocery stores in the greater Chicago area to send an overnight shipment of fresh parsley to our customers. We have one store in Chicago, IL and one store in Gary, IN both ready to help out with this request.

Last night, our on-call developer created a query to identify the impacted customers and their attributes in order to compose an offer to these customers to make things right. But the developer was paged at 2 a.m. when the problem occurred, and she created a fast query so that she could go back to sleep.

You review her code today and decide to reformat her query so that she can catch up on sleep.

Here is the query she emailed you. Refactor it to apply a consistent format, and add comments that explain your choices. We are going to review different options in the lecture, so if you are willing to share your refactored query with the class, then let us know!

CODE 
select 
    first_name || ' ' || last_name as customer_name,
    ca.customer_city,
    ca.customer_state,
    s.food_pref_count,
    (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
    (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
from vk_data.customers.customer_address as ca
join vk_data.customers.customer_data c on ca.customer_id = c.customer_id
left join vk_data.resources.us_cities us 
on UPPER(rtrim(ltrim(ca.customer_state))) = upper(TRIM(us.state_abbr))
    and trim(lower(ca.customer_city)) = trim(lower(us.city_name))
join (
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
) s on c.customer_id = s.customer_id
    cross join 
    ( select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL') chic
cross join 
    ( select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN') gary
where 
    ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%')
    and customer_state = 'KY')
    or
    (customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
    or
    (customer_state = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%')
*/

with customer_info as (
    select 
        customer_data.customer_id as customer_id,
        first_name || ' ' || last_name as customer_name,
        trim(customer_address.customer_city) as customer_city ,
        trim(customer_address.customer_state) as customer_state
    from customers.customer_data as customer_data
    inner join customers.customer_address as customer_address on customer_data.customer_id = customer_address.customer_id
)
--select * from customer_info
,active_customer_food_pref_count as (
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
)
-- select * from active_customer_food_pref_count
,chicago_geo_location as (
    select 
        geo_location as chicago_geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
)
--select * from chicago_geo_location
,gary_geo_location as (
    select 
        geo_location as gary_geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
)
--select * from gary_geo_location;
,cities as (
    select 
         cities.city_name,
         state_abbr,
         cities.geo_location,
         case 
             when
                 (state_abbr = 'KY' and trim(city_name) ilike any('%concord%','%georgetown%','%ashland%')) then true
             when 
                 (state_abbr = 'CA' and (trim(city_name) ilike any('%oakland%','%pleasant hill%'))) then true
             when 
                 (state_abbr = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%') then true
             else
                 false end as filter 
    from resources.us_cities as cities 
)
--select * from cities 
,join_cte_table as (
    select 
        customer_info.customer_name,
        customer_info.customer_city,
        customer_info.customer_state,
        active_customer_food_pref_count.food_pref_count,
        (st_distance(cities.geo_location,chicago_geo_location) / 1609)::int as chicago_distance_miles,
        (st_distance(cities.geo_location,chicago_geo_location) / 1609)::int as gary_distance_miles
    from customer_info 
    inner join active_customer_food_pref_count 
        on customer_info.customer_id = active_customer_food_pref_count.customer_id
    left join cities 
        on lower(customer_info.customer_city) = lower(cities.city_name)
        and upper(customer_info.customer_state) = upper(state_abbr)
    cross join chicago_geo_location
    cross join gary_geo_location
    where filter = true
)
select * from join_cte_table
