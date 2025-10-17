--- JOINS
CREATE TABLE LEFTtable(
Date DATE,
Countryid int,
units int

);

CREATE TABLE RIGHTtable(

id INT,
country VARCHAR(100)
);

INSERT INTO LEFTtable (Date, Countryid, units) VALUES
    ('1/1/2020', 1, 40),
    ('1/2/2020', 1, 25),
    ('1/3/2020', 3, 30),
    ('1/4/2020', 2, 35);


	INSERT INTO RIGHTtable (id, country) VALUES
    (3, 'Panama'),
    (4, 'Spain');

	SELECT * from LEFTtable;
	----inner
	SELECT L.Countryid,L.Date,L.units ,R.country FROM LEFTtable AS L
	inner join RIGHTtable AS R
	ON L.Countryid = R.id;
	----left
		SELECT L.Countryid,L.Date,L.units ,R.country FROM LEFTtable AS L
	left join RIGHTtable AS R
	ON L.Countryid = R.id;
	----right
		SELECT L.Countryid,L.Date,L.units ,R.country FROM LEFTtable AS L
	right join RIGHTtable AS R
	ON L.Countryid = R.id;
	---full
		SELECT L.Countryid,L.Date,L.units ,R.country FROM LEFTtable AS L
	full join RIGHTtable AS R
	ON L.Countryid = R.id;


	---left anti
	
	SELECT L.CountryID, L.Date, L.Units
FROM LeftTable AS L
LEFT JOIN RightTable AS R ON L.CountryID = R.ID
WHERE R.ID IS NULL;