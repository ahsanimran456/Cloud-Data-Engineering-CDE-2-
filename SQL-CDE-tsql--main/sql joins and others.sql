
SELECT
    city,
    COUNT (*)
FROM
    sales.customers
WHERE
    state = 'CA'
GROUP BY
    city
HAVING
    COUNT (*) > 5
ORDER BY
    city;
---- joining data

CREATE SCHEMA hr;
go
CREATE TABLE hr.candidates(
	id INT IDENTITY(1,1) PRIMARY KEY,
	fullname VARCHAR(100) NOT NULL
);

CREATE TABLE hr.employees(
	id INT IDENTITY(1,1) PRIMARY KEY,
	fullname VARCHAR(100) NOT NULL
);

INSERT INTO hr.candidates(fullname)
VALUES 
	('John Doe'),
    ('Lily Bush'),
    ('Peter Drucker'),
    ('Jane Doe');



INSERT INTO 
    hr.employees(fullname)
VALUES
    ('John Doe'),
    ('Jane Doe'),
    ('Michael Scott'),
    ('Jack Sparrow');

SELECT * FROM hr.employees;
SELECT * FROM hr.candidates;


----- LEFT TABLE ==> candidates
--    RIGHT TABLE ==> employees

-- INNER join

SELECT e.id,c.fullname
FROM hr.employees AS e
INNER JOIN hr.candidates AS c
ON e.fullname = c.fullname;
--- left join
SELECT e.id,c.fullname
FROM hr.employees AS e
LEFT JOIN hr.candidates AS c
ON e.id = c.id;

---Right join

SELECT e.id,c.fullname
FROM hr.employees AS e
RIGHT JOIN hr.candidates AS c
ON e.fullname = c.fullname;
---full join

SELECT e.id,c.fullname
FROM hr.employees AS e
FULL JOIN hr.candidates AS c
ON e.fullname = c.fullname;

SELECT * FROM sales.staffs;