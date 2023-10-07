----  Business Queries



--1. Top customer region

WITH cust_dim AS
(
  SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.CUSTOMER_DIM
),
reg_dim as (
  SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.REGION_DIM
),
final AS (
    SELECT * FROM
    (
        SELECT cust_id,
               region_id,
               SUM(total_price) as total_amount,
               DENSE_RANK() OVER(PARTITION BY region_id ORDER BY total_amount DESC) AS cust_rank
        FROM MOCK_PROJECT_DB.CONSUMPTION.ORDER_CUSTOMER_FACT
        GROUP BY cust_id,region_id
    ) AS derived
    WHERE derived.cust_rank<=3
)

SELECT cust_name,
       region_name,
       cust_rank,
       total_amount
FROM final ,cust_dim, reg_dim
WHERE final.cust_id = cust_dim.cust_id AND
      final.region_id = reg_dim.region_id
ORDER BY region_name, cust_rank



-- 2. Top customer halfyearly


WITH order_dim AS
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDERS_DIM
),
cust_dim AS
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.CUSTOMER_DIM
),
order_cust_fact AS
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDER_CUSTOMER_FACT
),
final AS
(
SELECT order_key, 
       cust_key, 
       cust_name, 
       order_date, 
       total_price,
       CASE WHEN MONTH(order_date) <=6 
            THEN concat(YEAR(order_date),'-h1') 
            ELSE concat(YEAR(order_date),'-h2')  
       END AS half_year_status
FROM order_dim,cust_dim,order_cust_fact
WHERE order_dim.order_id = order_cust_fact.order_id AND
      cust_dim.cust_id = order_cust_fact.cust_id
)

SELECT * FROM
(
    SELECT cust_name,
           half_year_status,
           SUM(total_price) AS total_amount,
           DENSE_RANK() OVER(PARTITION BY half_year_status ORDER BY total_amount DESC) AS cust_rank 
    FROM final
    GROUP BY cust_name,half_year_status 
) AS derived
WHERE derived.cust_rank<=3
ORDER BY half_year_status,cust_rank




-- 3. Top customer quaterly

-- Orders wise

WITH order_dim AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDERS_DIM
),
cust_dim AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.CUSTOMER_DIM
),
order_cust_fct AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDER_CUSTOMER_FACT
),
final AS
(
      SELECT 
            order_key, 
            cust_key, 
            cust_name, 
            order_date, 
            total_price,
            CASE  WHEN MONTH(order_date) <=3 
                  THEN 'Q1'
                  WHEN MONTH(order_date) <=6 
                  THEN 'Q2'
                  WHEN MONTH(order_date) <=9 
                  THEN 'Q3'
                  ELSE 'Q4'
            END AS quater,
            YEAR(order_date) AS YEARLY
      FROM order_dim,cust_dim,order_cust_fct
      WHERE order_dim.order_id = order_cust_fct.order_id AND
            cust_dim.cust_id = order_cust_fct.cust_id
)

SELECT * FROM
(
      SELECT 
            cust_name,
            quater,
            yearly,
            SUM(total_price) AS total_amount,
            DENSE_RANK() OVER(PARTITION BY quater, yearly ORDER BY total_amount DESC) AS cust_rank 
      FROM final
      GROUP BY cust_name,quater,yearly 
) AS derived
WHERE derived.cust_rank<=3
ORDER BY total_amount DESC


-- region wise

WITH order_dim AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDERS_DIM
),
cust_dim AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.CUSTOMER_DIM
),
order_cust_fct AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDER_CUSTOMER_FACT
),
region_dim AS (
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.REGION_DIM
),
final AS
(
      SELECT 
            order_key, 
            cust_key, 
            cust_name, 
            order_date, 
            region_name,
            total_price,
            CASE  WHEN MONTH(order_date) <=3 
                  THEN concat(YEAR(order_date),'-Q1')
                  WHEN MONTH(order_date) <=6 
                  THEN concat(YEAR(order_date),'-Q2')
                  WHEN MONTH(order_date) <=9 
                  THEN concat(YEAR(order_date),'-Q3')
                  ELSE concat(YEAR(order_date),'-Q4')
            END AS quaterly_status,
            CASE  WHEN MONTH(order_date) <=3 
                  THEN 'Q1'
                  WHEN MONTH(order_date) <=6 
                  THEN 'Q2'
                  WHEN MONTH(order_date) <=9 
                  THEN 'Q3'
                  ELSE 'Q4'
            END AS quater,
            YEAR(order_date) AS YEARLY
      FROM order_dim,cust_dim,order_cust_fct, region_dim
      WHERE order_dim.order_id = order_cust_fct.order_id AND
            cust_dim.cust_id = order_cust_fct.cust_id AND
            order_cust_fct.region_id = region_dim.region_id
)

SELECT * FROM
(
      SELECT 
            cust_name,
            quaterly_status,
            quater,
            yearly,
            region_name,
            SUM(total_price) AS total_amount,
            DENSE_RANK() OVER(PARTITION BY quaterly_status, region_name ORDER BY total_amount DESC) AS cust_rank 
      FROM final
      GROUP BY cust_name,quaterly_status,quater,yearly, region_name 
) AS derived
WHERE derived.cust_rank<=3
ORDER BY region_name,quaterly_status,total_amount DESC


-- nation wise


WITH order_dim AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDERS_DIM
),
cust_dim AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.CUSTOMER_DIM
),
order_cust_fct AS
(
      SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDER_CUSTOMER_FACT
),
nation_dim AS (
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.NATION_DIM
),
final AS
(
      SELECT 
            order_key, 
            cust_key, 
            cust_name, 
            order_date, 
            nation_name,
            total_price,
            CASE  WHEN MONTH(order_date) <=3 
                  THEN 'Q1'
                  WHEN MONTH(order_date) <=6 
                  THEN 'Q2'
                  WHEN MONTH(order_date) <=9 
                  THEN 'Q3'
                  ELSE 'Q4'
            END AS quater,
            YEAR(order_date) AS YEARLY
      FROM order_dim,cust_dim,order_cust_fct, nation_dim
      WHERE order_dim.order_id = order_cust_fct.order_id AND
            cust_dim.cust_id = order_cust_fct.cust_id AND
            order_cust_fct.nation_id = nation_dim.nation_id
)

SELECT * FROM
(
      SELECT 
            cust_name,
            yearly,
            quater,
            nation_name,
            SUM(total_price) AS total_amount,
            DENSE_RANK() OVER(PARTITION BY quater,yearly, nation_name ORDER BY total_amount DESC) AS cust_rank 
      FROM final
      GROUP BY cust_name,quater,yearly, nation_name 
) AS derived
WHERE derived.cust_rank<=3
ORDER BY nation_name,yearly, quater,total_amount DESC




-- 4. ship mode trends

WITH line_item AS 
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.LINE_ITEM_DIM
),
part_supp AS
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.PART_SUPPLIER_FACT
),
region AS
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.REGION_DIM
),
final AS
(
    SELECT ship_mode,
           ship_date,
           part_supp.region_id,
           region_key,
           region_name,
           order_id 
    FROM line_item,part_supp,region
    WHERE line_item.supp_id = part_supp.supp_id AND 
          line_item.part_id = part_supp.part_id AND
          part_supp.region_id = region.region_id
)

select YEAR(ship_date) as ship_year,
       region_key,
       region_name, 
       ship_mode, 
       COUNT(order_id) as num_orders
FROM final
WHERE ship_year >= YEAR(current_date()) - 5
GROUP BY ship_year,region_key,region_name,ship_mode
ORDER BY ship_year,region_key




-- 5. Most returning customer


WITH cust_dim AS (
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.CUSTOMER_DIM
),
order_cust_fact AS (
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.ORDER_CUSTOMER_FACT
), 
final AS ( 
    SELECT cust_key, 
           cust_name, 
           COUNT(order_id) AS num_orders,
           DENSE_RANK() OVER(ORDER BY num_orders DESC) AS cust_rank
    FROM cust_dim, order_cust_fact
    WHERE cust_dim.cust_id = order_cust_fact.cust_id  
    GROUP BY cust_key, cust_name
    ORDER BY num_orders DESC
)

SELECT 
    cust_key,
    cust_name,
    num_orders
FROM final
where cust_rank <=3
ORDER BY cust_rank 
limit 3





-- 6. Most supplier region

WITH part_supp AS
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.PART_SUPPLIER_FACT
),
region AS
(
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.REGION_DIM
),
final AS
(
    SELECT supp_id,
           region_key,
           region_name 
    FROM part_supp,region
    WHERE part_supp.region_id = region.region_id
)

SELECT * FROM
(
    SELECT region_key,
           region_name,
           COUNT(DISTINCT supp_id) total_supplier, 
           DENSE_RANK() OVER(ORDER BY total_supplier DESC) AS supp_rank
    FROM final
    GROUP BY region_key,region_name
    ORDER BY region_key
)




-- 7. Most popular brand

WITH line_item_dim AS (
    SELECT * FROM MOCK_PROJECT_DB.CONSUMPTION.LINE_ITEM_DIM
),
part_dim AS (
    select * from MOCK_PROJECT_DB.CONSUMPTION.PART_DIM
),
orders_dim AS (
    select * from MOCK_PROJECT_DB.CONSUMPTION.ORDERS_DIM
), 
final AS (
    SELECT part_brand, 
           order_date, 
           line_item_dim.order_id AS line_item_order_id
    FROM line_item_dim, part_dim, orders_dim
    WHERE line_item_dim.part_id = part_dim.part_id AND
          line_item_dim.order_id = orders_dim.order_id
) 

SELECT part_brand, 
       order_year, 
       num_orders,
       brand_rank
FROM(
    SELECT part_brand, 
           YEAR(order_date) AS order_year,
           COUNT(line_item_order_id) AS num_orders,
           DENSE_RANK() OVER(PARTITION BY order_year ORDER BY num_orders DESC) AS brand_rank
    FROM final
    GROUP BY part_brand, order_year
)
WHERE brand_rank<=3
ORDER BY order_year, num_orders DESC




-- 8. Collect cod orders
SELECT ship_instruct,
       COUNT(order_id) AS total_order_collect_cod
FROM MOCK_PROJECT_DB.CONSUMPTION.LINE_ITEM_DIM
GROUP BY ship_instruct


