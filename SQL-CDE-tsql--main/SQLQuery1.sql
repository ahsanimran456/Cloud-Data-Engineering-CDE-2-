USE BikeStores;

SELECT (first_name + '' + last_name)as full_name FROM sales.customers
WHERE zip_code >500
;
SELECT * FROM sales.orders
where customer_id in (
select customer_id from sales.customers where first_name like('[A-B]%') 
);

select * from sales.orders
where customer_id in (
select  customer_id  from sales.customers where  state = 'NY'
);

SELECT
    product_name,
    list_price,
    category_id
FROM
    production.products as p1
WHERE
    list_price IN (
        SELECT
            MAX (p2.list_price)
        FROM
            production.products as p2
        WHERE
            p2.category_id = p1.category_id
        GROUP BY
            p2.category_id
    )
ORDER BY
    list_price;


	SELECT 
    first_name,
    last_name
FROM
    sales.staffs
UNION ALL
SELECT
    first_name,
    last_name
FROM
    sales.customers;


SELECT 
    first_name, 
    last_name, 
    COUNT(*) AS occurrence_count
FROM
    (
        SELECT 
            first_name,
            last_name
        FROM
            sales.staffs
        UNION ALL
        SELECT
            first_name,
            last_name
        FROM
            sales.customers
    ) combined_names
GROUP BY 
    first_name, last_name
HAVING 
    COUNT(*) > 1;


