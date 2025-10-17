--grouping data

SELECT customer_id,
SUM(customer_id) AS order_placed
FROM sales.orders
GROUP BY  customer_id
ORDER BY customer_id;

SELECT
    customer_id,
    YEAR (order_date) order_year,
	COUNT (order_id) AS  order_placed
FROM
    sales.orders

GROUP BY
    customer_id,
    YEAR (order_date)
ORDER BY
    customer_id;


	SELECT DISTINCT
    customer_id,
    YEAR (order_date) order_year
FROM
    sales.orders
WHERE
    customer_id IN (1, 2)
ORDER BY
    customer_id;



	SELECT
    customer_id,
    YEAR (order_date) order_year,
    order_status
FROM
    sales.orders
WHERE
    customer_id IN (1, 2)
GROUP BY
    customer_id,
    YEAR (order_date)
ORDER BY
    customer_id;


	SELECT city,
	state,
	COUNT (customer_id) AS customer_count
	
	FROM sales.customers
	GROUP BY city,state
	ORDER BY city;


	SELECT brand_name,
	MIN(list_price) AS min_price,
	MIN(list_price)  AS max_price
	FROM production.products AS p
	INNER JOIN  production.brands AS b ON  b.brand_id=p.brand_id
	WHERE model_year = 2017
	GROUP BY brand_name
	ORDER BY brand_name;

	SELECT order_id ,SUM(list_price*quantity *(1-discount)) AS net_value
	FROM sales.order_items
	GROUP BY order_id;

	



	------GROUP SETS
	SELECT
    b.brand_name AS brand,
    c.category_name AS category,
    p.model_year,
    round(
        SUM (
            quantity * i.list_price * (1 - discount)
        ),
        0
    ) sales
	INTO sales.sales_summary
FROM
    sales.order_items i
INNER JOIN production.products p ON p.product_id = i.product_id
INNER JOIN production.brands b ON b.brand_id = p.brand_id
INNER JOIN production.categories c ON c.category_id = p.category_id
GROUP BY
    b.brand_name,
    c.category_name,
    p.model_year
ORDER BY
    b.brand_name,
    c.category_name,
    p.model_year;


	SELECT * FROM sales.sales_summary;

	SELECT
    brand,
    SUM (sales) sales
FROM
    sales.sales_summary
GROUP BY
    brand
ORDER BY
    brand;

	SELECT
    brand,
    category,
    SUM (sales) sales
FROM
    sales.sales_summary
GROUP BY
    brand,
    category
UNION ALL
SELECT
    brand,
    NULL,
    SUM (sales) sales
FROM
    sales.sales_summary
GROUP BY
    brand
UNION ALL
SELECT
    NULL,
    category,
    SUM (sales) sales
FROM
    sales.sales_summary
GROUP BY
    category
UNION ALL
SELECT
    NULL,
    NULL,
    SUM (sales)
FROM
    sales.sales_summary
ORDER BY brand, category;


----- also can be written as

SELECT
	brand,
	category,
	SUM (sales) sales
FROM
	sales.sales_summary
GROUP BY
	GROUPING SETS (
		(brand, category),
		(brand),
		(category),
		()
	);
--ORDER BY
--	brand,
--	category;



---------cube
SELECT
    brand,
    category,
    SUM (sales) sales
FROM
    sales.sales_summary
GROUP BY
    brand,
    CUBE(category);