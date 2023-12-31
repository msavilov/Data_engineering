DROP TABLE if exists STG_SUPERMARKET_SALES;
CREATE TABLE if not exists STG_SUPERMARKET_SALES(
invoice_ID varchar(128),
branch varchar(128),
city varchar(128),
customer_type varchar(128),
gender varchar(128),
product_line varchar(128),
unit_price varchar(128),
quantity varchar(128),
tax varchar(128),
total varchar(128),
date varchar(128),
time varchar(128),
payment varchar(128),
cogs varchar(128),
gross_margin_percentage varchar(128),
gross_income varchar(128),
rating varchar(128)
);