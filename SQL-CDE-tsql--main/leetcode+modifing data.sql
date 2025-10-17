select * from production.brands;
set identity_insert production.brands off
insert into production.brands(brand_id,brand_name) values (100,'honda')

CREATE TABLE production.products_demo (
	product_id INT IDENTITY (1, 1) PRIMARY KEY,
	product_name VARCHAR (255) NOT NULL,
	brand_id INT NOT NULL,
	category_id INT NOT NULL,
	model_year SMALLINT NOT NULL,
	list_price DECIMAL (10, 2) NOT NULL,
	FOREIGN KEY (category_id) REFERENCES production.categories (category_id) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (brand_id) REFERENCES production.brands (brand_id) ON DELETE CASCADE ON UPDATE CASCADE
);
set identity_insert production.products_demo on 
insert into production.products_demo(product_id,product_name,brand_id,category_id,model_year,list_price)select * from production.products;
select * from production.products_demo



update
production.products_demo
set
product_name = brand_name
from production.brands as c
inner join production.products_demo as b on
b.category_id =c.brand_id;

select * from production.products_demo as updated 
select * from production.brands