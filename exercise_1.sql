-- CTE with distinct city_name and state_abr for mapping
WITH city_info AS (
    SELECT
        DISTINCT LOWER(city_name) AS city_name,
        LOWER(state_abbr) AS state_name,
        geo_location AS location
    FROM
        resources.us_cities
),
-- CTE to get extract supplier geo location from available cities and states in city_info CTE.
city_info_supplier_info AS (
    SELECT
        si.supplier_id,
        si.supplier_name,
        ci.city_name AS supplier_city_name,
        ci.state_name AS supplier_state_name,
        ci.location AS supplier_location
    FROM
        suppliers.supplier_info si
        LEFT JOIN city_info AS ci
        ON LOWER(TRIM(si.supplier_city)) = LOWER(TRIM(ci.city_name))
        AND LOWER(TRIM(si.supplier_state)) = LOWER(TRIM(ci.state_name))
),
-- CTE to join customers table to location info by joining with the location CTE with unique cities and states
customer_info AS (
    SELECT
        ca.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        ci.location AS customer_location
    FROM
        customers.customer_address ca
        LEFT JOIN city_info ci
        ON LOWER(TRIM(ca.customer_city)) = LOWER(
            ci.city_name
        )
        AND LOWER(TRIM(ca.customer_state)) = LOWER(
            ci.state_name
        )
        INNER JOIN customers.customer_data cd
        ON cd.customer_id = ca.customer_id
),
-- CROSS JOIN supplier and customer table to get all combinations (all customers matched with all suppliers)
-- calculate the distance between each supplier and customer and use rank to find closest and furthestin supplier to each customer
final_table AS (
    SELECT
        *
    FROM
        city_info_supplier_info
        CROSS JOIN customer_info
),
-- CTE calculates distance
distance_table AS(
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        supplier_id,
        supplier_name,
        st_distance(
            customer_location,
            supplier_location
        ) / 1609 AS miles
    FROM
        final_table
),
final_query AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        supplier_id,
        supplier_name,
        miles,
        ROW_NUMBER() over (
            PARTITION BY customer_id
            ORDER BY
                miles
        ) AS ranked_distance
    FROM
        distance_table
) --final output and filtering for customers ranked 1 (closest supplier)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    supplier_id,
    supplier_name,
    miles
FROM
    final_query
WHERE
    miles IS NOT NULL
    AND ranked_distance = 1
ORDER BY
    last_name,
    first_name;
