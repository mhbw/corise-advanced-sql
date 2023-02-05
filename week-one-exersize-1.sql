/** 
EXERCISE 1

For our first exercise, we need to determine which customers are eligible to order from Virtual Kitchen, and which distributor will handle the orders that they place.  

at the top we'll be using the customer data, distro center data, and the supplied US_CITIES. From our EDA elsewhere we learned that the main mismatch was that the customers cities are lower case, where as US_CITIES has them as lower, so we know in advance that needs to be sorted

**/

-- First selecting customers as smaller subset
-- couple style notes: I like INITCAP over upper/lower for city, just stylistically, looks nicer
-- similarly, love Snowflakes ability to use USING for the joins over ON, very tidy
WITH customers AS 
(
SELECT customer_id
        , first_name 
        , last_name
        , email 
        , INITCAP(TRIM(customer_city)) AS customer_city
        , UPPER(customer_state) AS customer_state
    FROM
                vk_data.customers.customer_data
    INNER JOIN 
                  vk_data.customers.customer_address USING (customer_id)
),

-- selecting locations, distinct city||state only
-- sidebar: I don't really personally love this method of resolving to a single 
-- city state using the row_num because it's not going to be a true closest option, 
-- imho they should all be left in and pick the closest but we're accepting a few
-- handwavy items anyhow e.g. the cities having incorrect zips which would be the most
-- accurate anyway, but I digress.
-- I DO love qualify as a better Snowflake resolution to WHERE clauses that's great
 cities AS (
    select INITCAP(trim(city_name)) AS city
        , UPPER(state_abbr) AS state
        , lat
        , long
    FROM vk_data.resources.us_cities
    QUALIFY row_number() OVER (PARTITION BY INITCAP(city_name), UPPER(state_abbr) ORDER BY 1) = 1
),

-- selecting suppliers 
-- incorporating the lat long from the cities cte above
suppliers AS (
 SELECT supplier_id
        , supplier_name
        , INITCAP(trim(supplier_city)) AS supplier_city
        , UPPER(supplier_state) AS supplier_state
        , cities.lat AS supplier_lat
        , cities.long AS supplier_long
    FROM VK_DATA.suppliers.supplier_info
    INNER JOIN cities 
    ON VK_DATA.suppliers.supplier_info.supplier_city||VK_DATA.suppliers.supplier_info.supplier_state = cities.city||cities.state
),

-- narrowing customers to our eligible friends by dropping those who don't have a viable city dataset
-- another maybe nitpicky sidebar: I think this could be easier done by narrowing cities first, then distros 
-- then customers, so you're only handling each table once, but that's not what the project asks for per
-- se. I suppose we could override but we're here now.

narrowed_customers AS (
    SELECT *
    FROM
        customers
    INNER JOIN 
        cities
    ON
        customers.customer_city = cities.city and customers.customer_state =  cities.state
),


-- finding closest result
-- using cross join
final_form AS(
SELECT customer_id
        , first_name
        , last_name
        , email
        , supplier_id
        , supplier_name
        , ROUND((st_distance(st_makepoint(narrowed_customers.long, narrowed_customers.lat),        st_makepoint(suppliers.supplier_long, suppliers.supplier_lat)) / 1000), 2) as distance_km
    FROM 
        narrowed_customers
    CROSS JOIN 
        suppliers
    qualify row_number() over (partition by customer_id order by distance_km) = 1
	ORDER BY 3, 2
)
-- return results:

-- select count(*) FROM final_form returns 2,401

SELECT * FROM final_form;
