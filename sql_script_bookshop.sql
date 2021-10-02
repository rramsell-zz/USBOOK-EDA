/*
This query pipes/aggregates data across tables to provide a datasource for instights into sales populations.
Code is written in Hive SQL to be run in the bookshop schema. For primary key reference and table relationship insight, please refer to the 
ERD.

This data source is meant as an adhoc request and not to be implemented as an automated pipeline until stakeholder requests it.
*/

-- Creating the sales table with useful sales price data to be joined onto for bookshop data necessary to answer the three viz data challenge Q's
DROP TABLE if exists bookshop.sales purge;

CREATE TABLE if not exists bookshop.sales as
( 
   period date
 , isbn string
 , item_id string
 , order_id string
 , sale_price decimal (18,3)
 , price decimal (18,3)
 , discount decimal (18,3)
)
;

INSERT INTO bookshop.sales
(
   period
 , isbn
 , item_id
 , order_id
 , sale_price
 , price
 , discount	
)
SELECT x.sale_date as period
	, x.isbn
	, x.item_id
	, x.order_id
	, SUM(coalesce(y.price,0) - coalesce(x.discount,0)) as sale_price
	, SUM(y.price) as price
	, SUM(x.discount) as discount
FROM
(
SELECT *
FROM bookshop.sales_q1

UNION ALL

SELECT * 
FROM bookshop.sales_q2

UNION ALL

SELECT *
FROM bookshop.sales_q3

UNION ALL

SELECT *
FROM bookshop.sales_q4
) x
LEFT JOIN bookshop.edition y on (x.isbn = y.isbn)
GROUP BY x.sale_date
	   , x.isbn
;

-- This is to preserve memory space in the database . . . bookshop.sales_q1 through q4 have been replaced by bookshop.sales to include net sales
DROP TABLE if exists bookshop.sales_q1 purge;
DROP TABLE if exists bookshop.sales_q2 purge;
DROP TABLE if exists bookshop.sales_q3 purge;
DROP TABLE if exists bookshop.sales_q4 purge;

-- This query/table provides the data necessary to answer question 1 FROM the VIZ Data Challenge
CREATE TABLE if not exists bookshop.viz_data_question1 as
( 
   period date
 , book_id string
 , rating decimal(18,3)
 , sale_price decimal(18,3)
)
;
-- Insert statement for data source table viz_data_challenge
INSERT INTO bookshop.viz_data_question1
(
   period
 , book_id
 , rating
 , sale_price
)
SELECT i.period
	, ii.book_id
	, AVG(iii.rating) as rating
	, SUM(i.sale_price) as sale_price
FROM bookshop.sales i -- Sales Info
LEFT JOIN bookshop.edition ii on (i.isbn = ii.isbn) -- Book ID
LEFT JOIN bookshop.award iii on (ii.book_id = iii.book_id) -- Brings in Ratings
GROUP BY i.period
	 , i.book_id
;

-- This query/table provides the data necessary to answer question 2 FROM the VIZ Data Challenge
CREATE TABLE if not exists bookshop.viz_data_question2 as
( 
   period date
 , book_id string
 , genre string
 , award_name string
 , sale_price decimal(18,3)
)
;

-- Insert statement for data source table viz_data_challenge
INSERT INTO bookshop.viz_data_question2
(
   period
 , book_id
 , genre
 , award_name
 , sale_price
)

SELECT period
	 , book_id
 	 , genre
 	 , award_name
 	 , sale_price
FROM bookshop.sales i -- Sales Info
LEFT JOIN bookshop.edition ii on (i.isbn = ii.isbn) -- Book ID
LEFT JOIN bookshop.book iv on (iv.book_id = ii.book_id) -- Book Titles
LEFT JOIN (SELECT concat(book_id1, book_id2) as book_id FROM bookshop.info) v on (v.book_id = ii.book_id) -- Genre Info
LEFT JOIN bookshop.award vi on (iv.title = vi.title) -- Award Info
WHERE vi.award_name is not null AND vi.award_name <> ''
;


