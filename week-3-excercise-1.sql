-- The good: we've got one log. The bad: we've got kind of a lame log.
-- initial query didn't reveal much, so maybe we'll grab a couple unique event_details 
-- better inspect 

select distinct(event_details) from vk_data.events.website_activity limit 100;

-- from this we learn we've basically got 9 typical activities, which are landing on
-- the home page, searching for meals, or a few specific recipe_ids; personally I'd
-- guess that's not going to be the standard for long, we'll see a branching on some 
-- standard actions, but the core types of log items are here.

-- First we'll extract the events themselves and parse the json to an useable format
-- using the parse_json function [thanks, snowflake devs!]
-- The group by eliminates about 40 redundant rows, which is interesting
WITH events AS (
SELECT event_id
	, session_id
    , event_timestamp
    , TRIM(parse_json(event_details):"recipe_id", '"') AS recipe_id
    , TRIM(parse_json(event_details):"event",'"') as event_type
FROM vk_data.events.website_activity
GROUP BY 1,2,3,4,5
)


-- I enjoy the added conditional function in snowflake [IFF], gives some 
-- more flexibility as we're creating here.
, av_session AS (
SELECT session_id
	, MIN(event_timestamp) AS min_event_timestamp
    , MAX(event_timestamp) AS max_event_timestamp
    , IFF(COUNT_IF(event_type='view_recipe')=0,NULL, round(count_if(event_type='search')/count_if(event_type='view_recipe'))) AS searches_per_recipe_view
FROM
	events
GROUP BY session_id)

-- and now we pull in the most viewed recipe, which is our most straightforward part
, most_viewed AS (
SElECT date(event_timestamp) AS event_day
	, recipe_id
    , count(*) as total_views
FROM
	events
WHERE recipe_id IS NOT NULL
GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_day ORDER BY total_views DESC) = 1
)

-- now joining it all back for our 'report'
-- I confess I found this hardest since we don't have an easy key to join on
-- thanks to the TAs for pointing out we have other options, I was stuck in a midset
-- of only looking for one set ID on all tables, but that's not really always essential
-- I also don't feel rock solid that the average searches_per_view is what was meant there
-- but this is my best interpritation on line 61
, report AS (
SELECT DATE(min_event_timestamp) AS event_day
	, COUNT(session_id) AS total_sessions
    , ROUND(AVG(DATEDIFF('sec', min_event_timestamp, max_event_timestamp))) AS average_session_length
    , AVG(searches_per_recipe_view) AS avg_search_per_recipe_per_day
    , max(recipe_name) AS days_top_recipe
FROM
    av_session
 INNER JOIN
 most_viewed 
 ON
 	date(av_session.min_event_timestamp) = most_viewed.event_day
INNER JOIN
	chefs.recipe USING (recipe_id)
GROUP BY 1
)

-- I'm sorting by most recent day, since if I'm looking at a report
-- I want to see the most recent toplines
SELECT * FROM report
ORDER BY 1 DESC;
