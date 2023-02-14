/** EXERCIZE 2: refactor the code block to make it more inteligible and functional 
First, noting that our general preference in the course, our style guide if you will, 
is to go for CTEs, I think breaking this into a few smaller CTE type chunks will be 
preferred. Will re-evaluate as necissary. **/


-- I've decided to mostly keep the customers segment as one and then cross join
-- there's an argument to be made to break this up too but I don't think that's
-- really the point of our style guide. Plus it makes the narrowing by city faster
-- I did format the where segment to be cleaner, and generally I think there are 
-- redundant uses of the ilike feature here, I think you can do it as an 'IN' call
with customers as (
select first_name || ' ' || last_name as customer_name
    , ca.customer_city
    , ca.customer_state
    , ca.customer_id
from vk_data.customers.customer_address as ca
join vk_data.customers.customer_data c on ca.customer_id = c.customer_id
where 
    (customer_state = 'KY' and (trim(CUSTOMER_CITY) in ('Concord', 'Georgetown', 'Ashland')))
    or
    (customer_state = 'CA' and (trim(CUSTOMER_CITY) in('Oakland', 'Pleasant Hill')))
    or
    (customer_state = 'TX' and (trim(CUSTOMER_CITY) in('Arlington', 'Brownsville')))
),

-- fast count case when to pull in customer preferences    
preferences as (
select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),

-- I dislike this little element, I think we should be able to make one table and join once
-- but given the time constraint we're going to make two tables
chicago as (
   select 
        geo_location
    from
        vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'  

), 

gary as (
   select 
        geo_location
    from
        vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'   
)


-- tie it up as a little bow with distance and food
-- personally, I think a flaw here is still having to break out subtables for the geocode
-- given more time I'd like to simplify that cross join so it's only one
-- also, I know they want us to practice trim/lower but that segment seems extra
-- I made it one line
SELECT customers.*
    , preferences.food_pref_count
    , (st_distance(us.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles
    , (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
FROM
customers
JOIN
preferences on customers.customer_id = preferences.customer_id
LEFT JOIN vk_data.resources.us_cities us 
on UPPER(rtrim(ltrim(customers.customer_state)))||UPPER(trim(lower(customers.customer_city))) 
= UPPER(TRIM(us.state_abbr))||trim(UPPER(us.city_name))
CROSS JOIN chicago
CROSS JOIN gary
