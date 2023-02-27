/** Exercise 4: refactoring candidate SQL Part 2: Critique candidate:
I'm writing my critique at the top, which is listed as part 2, 
and then I'll refactor in the second part:

My takeaways here are there are a lot of costly joins, and possibliy 
as costly, the first CTE is messy and cross tables. The candidate may have
been technically successful, but it's not the tidiest thing and certainly
could be easily confused. 

I'm also not keen on these aliases. C is easily visually confused with O
as are P and l, even with non-serif text. Best to write something out.

PART 1: refactoring

My approach here is to minimize the joins, and do more CTEs
at the top. This should both simplify the query and speed it
up while also making it more human-readable. **/

-- First we'll pull in the automobile customers 
-- note we're also limiting to the essential columns
-- which will also improve performance
WITH automobile_customers AS (
SELECT c_custkey
FROM
	snowflake_sample_data.tpch_sf1.customer
WHERE c_mktsegment = 'AUTOMOBILE'
)

-- here I'm simplifying a lot of the work done on the orders. 
-- I'm using the qualifying to do the calcs here 
-- I do need to re-add in one join to limit to the auto_customers
-- but I'm at least going to use the CTE above to limit
-- the load on the first table
, urgent_orders as (
SELECT o_orderkey
	, o_custkey
    , o_orderdate
	, o_totalprice
FROM
	snowflake_sample_data.tpch_sf1.orders
INNER JOIN
	automobile_customers
ON
	tpch_sf1.orders.o_custkey = automobile_customers.c_custkey
WHERE
o_orderpriority = '1-URGENT'
)

-- pull in the line items so we can resolve the top 3
-- again, we're forced to do some join, but since it's
-- a smaller table, it will naturally be faster.
-- additionally narrowing to set columns is also a help
, relevant_line_items as (
SELECT l_orderkey
	, l_partkey
    , l_quantity
    , l_extendedprice
    , o_orderdate
    , o_custkey
    , o_orderkey
    , row_number() over (partition by c_custkey order by l_extendedprice desc) as price_rank
FROM
	snowflake_sample_data.tpch_sf1.lineitem
INNER JOIN
	urgent_orders
on 
	urgent_orders.o_orderkey = lineitem.l_orderkey
INNER JOIN
	automobile_customers
on
	automobile_customers.c_custkey = urgent_orders.o_custkey
QUALIFY price_rank <= 3
ORDER BY price_rank desc
)

-- I am going to wholesale use the applicants'
-- CTE here: I think it works fine for what it is

, top_orders as (
    select
    	o_custkey,
        max(o_orderdate) as last_order_date,
        listagg(o_orderkey, ', ') as order_numbers,
        sum(l_extendedprice) as total_spent
    from relevant_line_items
    where price_rank <= 3
    group by 1
    order by 1)

-- and now we combine it all together:
-- I wish there was a way to do this with lag or pivot but I've been
-- unable to find it so we have the clunky join solution here
SELECT top_orders.o_custkey
    , top_orders.last_order_date
    , top_orders.order_numbers
    , top_orders.total_spent
    , relevant_line_items.l_partkey as part_1_key
    ,  relevant_line_items.l_quantity as part_1_quantity
    , relevant_line_items.l_extendedprice as part_1_total_spent
    , relevant_line_items2.l_partkey as part_2_key
    , relevant_line_items2.l_quantity as part_2_quantity
    , relevant_line_items2.l_extendedprice as part_2_total_spent
    , relevant_line_items3.l_partkey as part_3_key
    , relevant_line_items3.l_quantity as part_3_quantity
    , relevant_line_items3.l_extendedprice as part_3_total_spent
FROM
	top_orders 
inner join relevant_line_items as relevant_line_items on top_orders.o_custkey = relevant_line_items.o_custkey
inner join relevant_line_items as relevant_line_items2 on top_orders.o_custkey = relevant_line_items2.o_custkey
inner join relevant_line_items as relevant_line_items3 on top_orders.o_custkey = relevant_line_items3.o_custkey
where relevant_line_items.price_rank = 1 and relevant_line_items2.price_rank = 2 and relevant_line_items3.price_rank = 3
order by top_orders.last_order_date desc
limit 100
