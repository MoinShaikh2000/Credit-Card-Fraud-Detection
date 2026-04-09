-- Select Database
USE datafinancial;

-- DATA UNDERSTANDING & CLEANING – CARDS TABLE

-- Create Staging Table (to preserve original data)
CREATE TABLE cards_staging LIKE cards_data;

INSERT INTO cards_staging
SELECT * FROM cards_data;

-- Initial Data Check
SELECT * FROM cards_staging;

-- REMOVE DUPLICATES

CREATE TABLE cards_cleaning AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY id, client_id, card_brand, card_type, card_number,
                            expires, cvv, has_chip, num_cards_issued,
                            credit_limit, acct_open_date,
                            year_pin_last_changed, card_on_dark_web
           ) AS rowwise
    FROM cards_staging
) AS card_table;

-- Check duplicates
SELECT *
FROM cards_cleaning
WHERE rowwise > 1;

-- Since no duplicates found
SELECT * FROM cards_cleaning;


-- CHECK DISTINCT VALUES

SELECT 
    COUNT(DISTINCT acct_open_date) AS acct_open_date_unique,
    COUNT(DISTINCT card_brand) AS card_brand_unique,
    COUNT(DISTINCT card_number) AS card_number_unique,
    COUNT(DISTINCT card_on_dark_web) AS card_on_dark_web_unique,
    COUNT(DISTINCT card_type) AS card_type_unique,
    COUNT(DISTINCT client_id) AS client_id_unique,
    COUNT(DISTINCT credit_limit) AS credit_limit_unique,
    COUNT(DISTINCT cvv) AS cvv_unique,
    COUNT(DISTINCT expires) AS expires_unique,
    COUNT(DISTINCT has_chip) AS has_chip_unique,
    COUNT(DISTINCT id) AS id_unique,
    COUNT(DISTINCT num_cards_issued) AS num_cards_issued_unique,
    COUNT(DISTINCT rowwise) AS rowwise_unique,
    COUNT(DISTINCT year_pin_last_changed) AS year_pin_last_changed_unique
FROM cards_cleaning;

-- Validate categorical values
SELECT DISTINCT card_type FROM cards_cleaning;

-- CHECK NULL VALUES


SELECT 
    SUM(CASE WHEN acct_open_date IS NULL THEN 1 ELSE 0 END) AS acct_open_date_nulls,
    SUM(CASE WHEN card_brand IS NULL THEN 1 ELSE 0 END) AS card_brand_nulls,
    SUM(CASE WHEN card_number IS NULL THEN 1 ELSE 0 END) AS card_number_nulls,
    SUM(CASE WHEN card_on_dark_web IS NULL THEN 1 ELSE 0 END) AS card_on_dark_web_nulls,
    SUM(CASE WHEN card_type IS NULL THEN 1 ELSE 0 END) AS card_type_nulls,
    SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS client_id_nulls,
    SUM(CASE WHEN credit_limit IS NULL THEN 1 ELSE 0 END) AS credit_limit_nulls,
    SUM(CASE WHEN cvv IS NULL THEN 1 ELSE 0 END) AS cvv_nulls,
    SUM(CASE WHEN expires IS NULL THEN 1 ELSE 0 END) AS expires_nulls,
    SUM(CASE WHEN has_chip IS NULL THEN 1 ELSE 0 END) AS has_chip_nulls,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,
    SUM(CASE WHEN num_cards_issued IS NULL THEN 1 ELSE 0 END) AS num_cards_issued_nulls,
    SUM(CASE WHEN rowwise IS NULL THEN 1 ELSE 0 END) AS rowwise_nulls,
    SUM(CASE WHEN year_pin_last_changed IS NULL THEN 1 ELSE 0 END) AS year_pin_last_changed_nulls
FROM cards_cleaning;

--  DATA TYPE CHECK


SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'cards_cleaning';

--  DATE FORMAT CLEANING

-- Fix expires column
SELECT expires,
       STR_TO_DATE(CONCAT('01/', expires), '%d/%m/%Y') AS formatted_date
FROM cards_cleaning;

-- Apply update
UPDATE cards_cleaning
SET expires = STR_TO_DATE(CONCAT('01/', expires), '%d/%m/%Y');

-- Fix acct_open_date column
UPDATE cards_cleaning
SET acct_open_date = STR_TO_DATE(CONCAT('01/', acct_open_date), '%d/%m/%Y');

-- FINAL CLEANUP


ALTER TABLE cards_cleaning
DROP COLUMN rowwise;

-- Final cleaned data
SELECT * FROM cards_cleaning;

-- FINAL STATUS
-- =============================================
-- Cards table successfully cleaned
-- =============================================

Select * from users_data;

-- CREATE STAGING TABLE


CREATE TABLE users_staging LIKE users_data;

INSERT INTO users_staging
SELECT * FROM users_data;



--  REMOVE DUPLICATES

CREATE TABLE users_cleaning AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
    FROM users_staging
) t
WHERE rn = 1;

-- CLEAN INCOME COLUMN
-- Remove '$' and convert to numeric
UPDATE users_cleaning
SET per_capita_income = REPLACE(per_capita_income, '$', '');

ALTER TABLE users_cleaning
MODIFY per_capita_income INT;


-- CHECK INVALID RETIREMENT AGE

SELECT *
FROM users_cleaning
WHERE retirement_age < current_age;

-- VALIDATE LAT/LONG

SELECT *
FROM users_cleaning
WHERE latitude NOT BETWEEN -90 AND 90
   OR longitude NOT BETWEEN -180 AND 180;

-- HANDLE NULLS
-- 

SELECT 
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,
    SUM(CASE WHEN current_age IS NULL THEN 1 ELSE 0 END) AS age_nulls,
    SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) AS gender_nulls,
    SUM(CASE WHEN per_capita_income IS NULL THEN 1 ELSE 0 END) AS income_nulls
FROM users_cleaning;

-- FINAL CLEAN TABLE

ALTER TABLE users_cleaning
DROP COLUMN rn;


-- =============================================
-- Userd table successfully cleaned
-- =============================================

-- Create staging table
CREATE TABLE transactions_staging LIKE transactions_data;

INSERT INTO transactions_staging
SELECT * FROM transactions_data;

-- Inspect data
SELECT * FROM transactions_staging;

-- Remove duplicates
CREATE TABLE transactions_cleaning AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY id, date, client_id, card_id, amount,
                            use_chip, merchant_id, merchant_city,
                            merchant_state, zip
               ORDER BY id
           ) AS rn
    FROM transactions_staging
) t
WHERE rn = 1;

-- Clean amount column (remove '$' and convert to numeric)
UPDATE transaction_cleaning
SET amount = REPLACE(amount, '$', '');

ALTER TABLE transactions_cleaning
MODIFY amount DECIMAL(10,2);

-- Validate negative transactions (important in financial data)
SELECT *
FROM transactions_cleaning
WHERE amount < 0;

-- Standardize transaction type
UPDATE transactions_cleaning
SET use_chip = LOWER(TRIM(use_chip));

-- Optional normalization
UPDATE transactions_cleaning
SET use_chip = 'swipe'
WHERE use_chip LIKE '%swipe%';

UPDATE transactions_cleaning
SET use_chip = 'chip'
WHERE use_chip LIKE '%chip%';

-- Convert date column to proper datetime (if not already)
ALTER TABLE transactions_cleaning
MODIFY date DATETIME;

-- Validate null values
SELECT 
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS date_nulls,
    SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS client_id_nulls,
    SUM(CASE WHEN card_id IS NULL THEN 1 ELSE 0 END) AS card_id_nulls,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_nulls,
    SUM(CASE WHEN merchant_id IS NULL THEN 1 ELSE 0 END) AS merchant_id_nulls
FROM transactions_cleaning;

-- Validate ZIP codes (remove decimals issue)
UPDATE transactions_cleaning
SET zip = CAST(zip AS UNSIGNED);

ALTER TABLE transactions_cleaning
MODIFY zip INT;

-- Check geographic consistency
SELECT *
FROM transactions_cleaning
WHERE merchant_state IS NULL
   OR merchant_city IS NULL;

-- Final cleanup
ALTER TABLE transactions_cleaning
DROP COLUMN rn;

-- Final dataset
SELECT * FROM transactions_cleaning;

-- ==============================================
-- Starting with the analysis
-- ==============================================

-- Final dataset
SELECT * FROM transactions_cleaning;
-- Here Id is a unique Identfier , Client Id refers to user ID

select * from user_cleaning;
-- Here Id is a unique Identifer

Select * from cards_cleaning;
-- Here Id is a unique Identfier , Client Id refers to user ID, card ID Refers to card ID

alter table user_cleaning
rename column id to user_id;

alter table cards_cleaning
rename column id to card_unique_id;

alter table cards_cleaning
rename column client_id to client_id2;

-- Joining all tables

create table Master_data as 
select *
from transaction_cleaning as t
left join cards_cleaning as c
on t.card_id = c.card_unique_id 
left join user_cleaning as u
on t.client_id = u.user_id;

--  Removing some basic colums from the table
alter table master_data
drop column client_id2;

alter table master_data
drop column card_unique_id ;

-- Starting with basic analysis for fraud

select * from master_data;

-- ====================================
-- Fraud Analysis
-- ====================================
-- Lets check for transactions where cards on dark web were used

select * from master_data
where card_on_dark_web = "Yes";

-- Good news no cards on dark web


-- Idectifying transation where total debt is more there yearly income, red flag

select id, date , card_id, amount, total_debt, yearly_income from master_data
where total_debt > yearly_income
order by total_debt desc;

-- Identifying user who are using swipe payment even after having chip

select * 
from master_data
where has_chip = "YES" and use_chip = "Swipe Transaction" and amount > 50;

-- identifying users who are user who are using alot of money comparatively to there per captia income

select user_id, amount, per_capita_income, yearly_income , Round((amount/per_capita_income)*100,2) as amount_percent
from master_data
where (amount/per_capita_income)*100 > 20
and per_capita_income > 0
order by amount_percent desc;

-- identifying user that have transaction amount more than there credit limits
select user_id, amount, credit_limit
from master_data
where amount > credit_limit
order by credit_limit desc;


select user_id, amount, credit_limit, 
	sum(amount) over (partition by user_id) as total_spent
from master_data
where amount > credit_limit
order by credit_limit desc;

select * 
from  (
select user_id, amount, credit_limit, 
	sum(amount) over (partition by user_id) as total_spent
from master_data
) as Total_spent 
where amount > credit_limit
order by total_spent desc;

-- ============================================================
-- Idenifying user who never updated PIN AND have over-limit transactions 
-- ============================================================

SELECT client_id, credit_score, yearly_income, total_debt, year_pin_last_changed,
    (YEAR(CURDATE()) - year_pin_last_changed) AS years_since_pin_change,
    COUNT(client_id) AS total_transactions,
    SUM(CASE WHEN amount > credit_limit THEN 1 ELSE 0 END) AS over_limit_transactions,
    ROUND(SUM(amount), 2) AS total_spent

FROM master_data

GROUP BY
    client_id, credit_score, yearly_income, total_debt, year_pin_last_changed

HAVING over_limit_transactions > 0
   AND years_since_pin_change >= 10

ORDER BY over_limit_transactions DESC, years_since_pin_change DESC;

-- ============================================================
-- Query 3: Customers whose spending spiked significantly
-- compared to their own prior year — behavioral anomaly
-- ============================================================

-- ============================================================
-- FRAUD ANALYSIS | Project 1: Credit Card Fraud Detection
-- Query 3: Customers whose spending spiked significantly
-- compared to their own prior year — behavioral anomaly
-- ============================================================

select year(date) as year, client_id, amount,
sum(amount) over(partition by year(date), client_id order by (amount)) as anual_spent
from master_data;

-- lets understand how much each user is spending per year

select year(date) as year, client_id, sum(amount) as yearly_spend
from master_data
group by year(date), client_id
order by year(date), sum(amount) desc
;


-- Now we know there annual spending we can compare the anual spending with there annual spending last year

with annual_user_spending
as
(
select year(date) as year, client_id, sum(amount) as yearly_spend
from master_data
group by year(date), client_id
order by year(date), sum(amount) desc
),
last_year_spending 
as
(select * , lag (yearly_spend,1,0)over(partition by client_id order by year) as previos_year_spend
from annual_user_spending
),
Change_rate 
as
(
select *, 
Round ((yearly_spend - previos_year_spend) / previos_year_spend * 100,2)  as change_perc_in_spend 
from last_year_spending
)
select * from change_rate
where yearly_spend > 10
and yearly_spend > previos_year_spend
AND previos_year_spend > 0
and change_perc_in_spend > 50;

-- ================================================

with annual_user_spending
as
(
select year(date) as year, client_id, credit_score, yearly_income, sum(amount) as yearly_spend
from master_data
group by year(date), client_id, credit_score, yearly_income
order by year(date), sum(amount) desc
),
last_year_spending 
as
(select * , lag (yearly_spend,1,0)over(partition by client_id order by year) as previos_year_spend
from annual_user_spending
),
Change_rate 
as
(
select *, 
Round ((yearly_spend - previos_year_spend) / previos_year_spend * 100,2)  as change_perc_in_spend 
from last_year_spending
)
select * from change_rate
where yearly_spend > 10
and yearly_spend > previos_year_spend
AND previos_year_spend > 0
and change_perc_in_spend > 50;


-- ==================================================
-- Phase two 
-- ==================================================
-- Evalutng the total ammount recieved by merchants per year

select * from master_data;

-- Identifying the merchants witht the most amount of overlimit transaction.

select merchant_id, year(date) as year, sum(amount) as merchant_total
from master_data
group by merchant_id, year(date) 
order by year(date), sum(amount) desc;

-- Lets first find the overlimit transactions

select client_id, amount, year(date), merchant_id, credit_limit, sum(amount) over(partition by merchant_id order by year(date)) as total_amount
from master_data
order by client_id, year(date),total_amount desc;

-- Now lets identify merchants where the crossing of credit limit is high
with 
credit_flag as
(
select *, 
case when credit_limit < amount then 1 else 0
end as credit_cross_flag
from master_data
),
total_transaction as
(
select merchant_id, merchant_city, merchant_state, count(*) as transaction_count,  sum(credit_cross_flag) as flag_count
from credit_flag
group by merchant_id, merchant_city, merchant_state
order by sum(credit_cross_flag) desc
)
select *, Round(flag_count/transaction_count*100,2) as overlimit_perc
from total_transaction
where flag_count >0
order by  transaction_count desc;

-- Identifying customer who are transaction abnormally high

with 
client_transaction as
(
select client_id, count(distinct id) as transaction_by_client ,
count(distinct card_id) as cards_used_by_client,
sum(amount) as total_amount,
round((sum(amount)/count(id)),2)as avg_transaction_amount
from master_data
group by client_id
order by count(distinct id) desc 
),
client_flagging as
(
select *, case when (avg(transaction_by_client) over() < transaction_by_client) then 1 else 0
end as client_flag
from client_transaction
)
select * from client_flagging
where client_flag = 1;

-- Identifying having debt higher than there yearly income


with user_risk_metrics
as
(
Select client_id, total_debt, yearly_income, credit_score, 
Round(total_debt/yearly_income,2) as Debt_to_income_ratio,count(id) as transaction_count, sum(amount) as total_amount_spent
from master_data
group by client_id, total_debt, yearly_income, credit_score
order by count(id) desc
)
select *, 
Case 
when total_debt > yearly_income and 
credit_score <650 and 
transaction_count> avg(transaction_count)over()
then 1 else 0
end as debt_risk_flag
from user_risk_metrics;

-- Based on our analysis creating a final risk score 

select * from master_data;
-- FLAG 1
select client_id,
max(
case when
amount>credit_limit
then 1 else 0 
end
) as transaction_beyond_limit
from master_data
group by client_id;

-- FLAG 2
SELECT client_id,
    max(
    Case when 
    (YEAR(CURDATE()) - year_pin_last_changed)>=10 
    then 1 else 0
    end
    ) as Not_updated_pin
FROM master_data
GROUP BY
    client_id;

-- FLAG 3

with annual_user_spending
as
(
select year(date) as year, client_id, sum(amount) as yearly_spend
from master_data
group by year(date), client_id
order by year(date), sum(amount) desc
),
last_year_spending 
as
(select * , lag (yearly_spend,1,0)over(partition by client_id order by year) as previos_year_spend
from annual_user_spending
),
Change_rate 
as
(
select *, 
Round ((yearly_spend - previos_year_spend) / previos_year_spend * 100,2)  as change_perc_in_spend 
from last_year_spending
)
select client_id, 
max(
case
when change_perc_in_spend > 50 
then 1 else 0
end
) as yearly_spike
from change_rate
group by client_id;

-- FLAG 4

with 
client_transaction as
(
select client_id, count(distinct id) as transaction_by_client ,
count(distinct card_id) as cards_used_by_client,
sum(amount) as total_amount,
round((sum(amount)/count(id)),2)as avg_transaction_amount
from master_data
group by client_id
order by count(distinct id) desc 
),
client_flagging as
(
select *, case when (avg(transaction_by_client) over() < transaction_by_client) then 1 else 0
end as client_flag
from client_transaction
)
select client_id, client_flag
from client_flagging;

-- FLAG 5
with user_risk_metrics
as
(
Select client_id, total_debt, yearly_income, credit_score, 
Round(total_debt/yearly_income,2) as Debt_to_income_ratio,count(id) as transaction_count, sum(amount) as total_amount_spent
from master_data
group by client_id, total_debt, yearly_income, credit_score
order by count(id) desc
)
select client_id, 
Case 
when total_debt > yearly_income and 
credit_score <650 and 
transaction_count> avg(transaction_count)over()
then 1 else 0
end as debt_risk_flag
from user_risk_metrics;

-- =================================
-- Final Analysis and Risk scoreing 
-- =================================

with 
flag1 as
(
select client_id,
max(
case when
amount>credit_limit
then 1 else 0 
end
) as transaction_beyond_limit
from master_data
group by client_id
),
flag2 as
(
SELECT client_id,
    max(
    Case when 
    (YEAR(CURDATE()) - year_pin_last_changed)>=10 
    then 1 else 0
    end
    ) as Not_updated_pin
FROM master_data
GROUP BY
    client_id
),
annual_user_spending
as
(
select year(date) as year, client_id, sum(amount) as yearly_spend
from master_data
group by year(date), client_id
order by year(date), sum(amount) desc
),
last_year_spending 
as
(select * , lag (yearly_spend,1,0)over(partition by client_id order by year) as previos_year_spend
from annual_user_spending
),
Change_rate 
as
(
select *, 
Round ((yearly_spend - previos_year_spend) / previos_year_spend * 100,2)  as change_perc_in_spend 
from last_year_spending
),
flag3 as
(
select client_id, 
max(
case
when change_perc_in_spend > 50 
then 1 else 0
end
) as yearly_spike
from change_rate
group by client_id
),
client_transaction as
(
select client_id, count(distinct id) as transaction_by_client ,
count(distinct card_id) as cards_used_by_client,
sum(amount) as total_amount,
round((sum(amount)/count(id)),2)as avg_transaction_amount
from master_data
group by client_id
order by count(distinct id) desc 
),
client_flagging as
(
select *, case when (avg(transaction_by_client) over() < transaction_by_client) then 1 else 0
end as client_flag
from client_transaction
),
flag4 as
(
select client_id, client_flag
from client_flagging
),
user_risk_metrics
as
(
Select client_id, total_debt, yearly_income, credit_score, 
Round(total_debt/yearly_income,2) as Debt_to_income_ratio,count(id) as transaction_count, sum(amount) as total_amount_spent
from master_data
group by client_id, total_debt, yearly_income, credit_score
order by count(id) desc
),
flag5 as
(
select client_id, 
Case 
when total_debt > yearly_income and 
credit_score <650 and 
transaction_count> avg(transaction_count)over()
then 1 else 0
end as debt_risk_flag
from user_risk_metrics
),
joined_flags as
(
select 
f1.client_id, f1.transaction_beyond_limit,
f2.Not_updated_pin,
f3.yearly_spike,
f4.client_flag,
f5.debt_risk_flag
from flag1 as f1
left join flag2 as f2 on
f1.client_id = f2.client_id
left join flag3 as f3 on
f1.client_id = f3.client_id
left join flag4 as f4 on
f1.client_id = f4.client_id
left join flag5 as f5 on
f1.client_id = f5.client_id
)
select *,
(COALESCE(transaction_beyond_limit,0) + 
 COALESCE(Not_updated_pin,0) + 
 COALESCE(yearly_spike,0) + 
 COALESCE(client_flag,0) + 
 COALESCE(debt_risk_flag,0)) as risk_score, 
 
round(((COALESCE(transaction_beyond_limit,0) + 
 COALESCE(Not_updated_pin,0) + 
 COALESCE(yearly_spike,0) + 
 COALESCE(client_flag,0) + 
 COALESCE(debt_risk_flag,0))/5)*100,2) as risk_percent,
 CASE 
    WHEN (COALESCE(transaction_beyond_limit,0) + 
 COALESCE(Not_updated_pin,0) + 
 COALESCE(yearly_spike,0) + 
 COALESCE(client_flag,0) + 
 COALESCE(debt_risk_flag,0)) >= 4 THEN 'Critical'
    WHEN (COALESCE(transaction_beyond_limit,0) + 
 COALESCE(Not_updated_pin,0) + 
 COALESCE(yearly_spike,0) + 
 COALESCE(client_flag,0) + 
 COALESCE(debt_risk_flag,0)) = 3 THEN 'High'
    WHEN (COALESCE(transaction_beyond_limit,0) + 
 COALESCE(Not_updated_pin,0) + 
 COALESCE(yearly_spike,0) + 
 COALESCE(client_flag,0) + 
 COALESCE(debt_risk_flag,0)) = 2 THEN 'Medium'
    ELSE 'Low'
END AS risk_category
from joined_flags





