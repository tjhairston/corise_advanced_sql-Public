
with customers as (
select
    customer_id,
    first_name as customer_first_name,
    last_name as customer_last_name,
    email as customer_email,
    lower(trim(customer_city)) as customer_city,
    lower(customer_state) as customer_state
from customers.customer_data
inner join customers.customer_address using (customer_id)
),
-- cleaning up and selecting what we need
cities as (
select
    lower(trim(city_name)) as city,
    lower(state_abbr) as state,
    lat,
    long
from resources.us_cities
    /*
     order by 1 because I need to order by something for the row_number, but
     here does not really matter as I'm just interested in city_name and
     state_abbr
     */
qualify row_number() over (partition by lower(city_name), lower(state_abbr) order by 1) = 1

),

eligible_customers as (
-- only eligible customers are happy customers!
    select
        *
    from customers
    /*
     Inner join as we only want customers with matches
     to the cities table
     */
    inner join cities
        on customers.customer_city = cities.city
        and customers.customer_state = cities.state
),

 
top_3_preferences as (
select 
    cs.customer_id as customer_id,
    lower(trim(rt.tag_property)) as customer_preference,
    rank() over (partition by customer_id order by lower(trim(rt.tag_property))) as tag_rank
from customers.customer_survey as cs 
inner join resources.recipe_tags rt
on cs.tag_id = rt.tag_id
where  cs.is_active = true
--get rid of duplicates coming from triming
group by customer_id, customer_preference
qualify tag_rank <= 3
),
pivot_preferences as (
 select
    customer_id,
    "1" as food_preference_1,
    "2" as food_preference_2,
    "3" as food_preference_3
from top_3_preferences 
    pivot(max(customer_preference) 
          for tag_rank in (1, 2, 3))
),
-- getting all the tags from recipes
recipe_tags as (
  select
    recipe_id,
    recipe_name,
    trim(replace(flat_tag.value, '"', '')) as recipe_tag
from chefs.recipe, table(flatten(tag_list)) as flat_tag
),
recommended_recipe as (
  select
    customer_id,
    min(recipe_name) as recipe
from pivot_preferences
    inner join recipe_tags on pivot_preferences.food_preference_1 = recipe_tags.recipe_tag
    group by 1
),
results as (
-- put everything together
select
    customer_id,
    customer_email,
    customer_first_name,
    food_preference_1,
    food_preference_2,
    food_preference_3,
    recipe
from eligible_customers
inner join recommended_recipe using(customer_id)
inner join pivot_preferences using(customer_id)
order by customer_email
)
select * from results;
