/** EXERCISE 2: Email Marketing Campaign
The goal is to create a recipie recommendation for an email marketing campaign: I'm going to recycle the CTEs from the 
first exercise here. If you're reviewing my code and also saw part one, feel free to skip to line 74 where the new 
material begins. Then we'll create a customer table with their top three choices, We're going to join the narrowed
customers back to the survey data, rank them and serve up a matching offer to entice them to buy. 

 Couple style notes: I like INITCAP over upper/lower for city, just stylistically, looks nicer similarly, 
 I love Snowflakes ability to use USING for the joins over ON, very tidy.

**/

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

-- meat of it here, we're now adding in their tags. This should result in multimple rows with given tags
-- in the sunday session the coach recommended these methods so I feel fine about it
customers_con_tags AS (
SELECT customer_id
    , first_name
    , last_name
    , email
    , TRIM(recipe_tags.tag_property) as tag_customer
    , ROW_NUMBER() OVER (PARTITION BY narrowed_customers.customer_id ORDER BY trim(recipe_tags.tag_property)) as rank_tag
FROM 
    narrowed_customers
INNER JOIN 
    vk_data.customers.customer_survey using (customer_id)
INNER JOIN 
    vk_data.resources.recipe_tags using (tag_id)
qualify rank_tag <= 3
),

-- here we'll use our pivot functions to flatten out the above
preferences as (
SELECT * from customers_con_tags
PIVOT(max(tag_customer) for rank_tag in (1,2,3))
    as pivot_values(customer_id
                    , first_name
                    , last_name
                    , email
                    , preference1 
                    , preference2 
                    , preference3 )
    
    ),



-- pull in recipies that map to said tags

recipes AS (
SELECT recipe_name
    , TRIM(REPLACE(tl.value, '"','')) AS tag
FROM
    vk_data.chefs.recipe,
    table(flatten(vk_data.chefs.recipe.tag_list)) AS tl
),

-- map the last part
customer_recipe AS(
SELECT customer_id
    , any_value(recipes.recipe_name) as recipe
FROM customers_con_tags
INNER JOIN
    recipes
    ON customers_con_tags.tag_customer = recipes.tag AND customers_con_tags.rank_tag =1
GROUP BY 1
)

-- final step, return usable data
-- tinying up the recipe name just incase someone dumps it right into an email send
SELECT preferences.customer_id
      , first_name
      , last_name
      , email
      , preference1
      , preference2
      , preference3
      , INITCAP(trim(customer_recipe.recipe)) as suggested_recipe
FROM
    preferences
LEFT JOIN
    customer_recipe using (customer_id)
ORDER BY email;
