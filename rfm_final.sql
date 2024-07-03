-- Create a temp table to store the rfm scores
create temp table rfm_score
(
segment varchar,
scores varchar	
);


-- Insert values into the temp table rfm_score 
insert into rfm_score
values
('Champions', '445, 454, 455, 544, 545, 554, 555'),
('Loyal', '335, 344, 345, 354, 355, 435, 444, 543'),
('Potential Loyalist', '323, 333, 341, 342, 351, 352, 353, 423, 431, 432, 433, 441, 442, 451, 452, 453, 531, 532, 533, 541, 542, 551, 552, 553'),
('New Customers', '311, 411, 412, 421, 422, 511, 512'),
('Promising', '313, 314, 315, 413, 414, 415, 424, 425, 513, 514, 515, 521, 522, 523, 524, 525'),
('Need Attention', '324, 325, 334, 343, 434, 443, 534, 535'),
('About To Sleep', '213, 221, 231, 241, 251, 312, 321, 331'),
('At Risk', '124, 125, 133, 134, 135, 142, 143, 145, 152, 153, 224, 225, 234, 235, 242, 243, 244, 245, 252, 253, 254, 255'),
('Cannot Lose Them', '113, 114, 115, 144, 154, 155, 214, 215'),
('Hibernating customers', '122, 123, 132, 211, 212, 222, 223, 231, 232, 233, 241, 251, 322, 332'),
('Lost customers', '111, 112, 121, 131, 141, 151');


-- Create a table to score the RFM scores after splitting the values with ','
create table segment_score
as(
select segment, regexp_split_to_table(scores, ', ')
from rfm_score_1
);


-- Chaging the column's name of the segment_score table to a proper name - scores
alter table segment_score
rename column regexp_split_to_table to scores;


-- Change data types of all the column to its corresponding types
ALTER TABLE sales_dataset_rfm_prj
ALTER COLUMN ordernumber  TYPE integer USING (ordernumber::integer),
ALTER COLUMN quantityordered  TYPE integer USING (quantityordered::integer),
ALTER COLUMN orderlinenumber  TYPE integer USING (orderlinenumber::integer),
ALTER COLUMN sales TYPE numeric USING (sales::numeric),
ALTER COLUMN msrp  TYPE integer USING (msrp::integer),
ALTER COLUMN priceeach TYPE numeric USING (priceeach::numeric),
ALTER COLUMN orderdate TYPE DATE using (orderdate::date);


-- Check for null/blank values 
select *
from sales_dataset_rfm_prj
where row(ordernumber, quantityordered, priceeach,
		orderlinenumber, sales, orderdate) is null 
-- There is no null values 


-- Check for duplicate values
with row_num as (
		
	select 
		row_number() over (partition by ordernumber, quantityordered, 
							priceeach, orderlinenumber, sales order by orderdate) as row_number_check
	from sales_dataset_rfm_prj
)

select *
from row_num
where row_number_check >1
-- There is no duplicate values

	

-- Identify OUTLIERS for QUANTITYORDERED column and solution for it
--Calculate percentile 25 and percentile 75
with percentile as(
	select percentile_cont(0.25) within group (order by QUANTITYORDERED) as pct_25_q1,
			percentile_cont(0.75) within group (order by QUANTITYORDERED) as pct_75_q3,
			percentile_cont(0.75) within group (order by QUANTITYORDERED) - 
				percentile_cont(0.25) within group (order by QUANTITYORDERED) as iqr
	from sales_dataset_rfm_prj
),

-- Calculate min and max values based on percentile 25 and percentile 75 values
min_max_values as (
	select pct_25_q1 - 1.5*iqr as min_value,
			pct_75_q3 + 1.5*iqr as max_value
	from percentile
),

-- Identify outliers with values smaller than min and bigger than max
outliers as (
	select quantityordered
	from sales_dataset_rfm_prj
	where quantityordered < (select min_value from min_max_values) or
			quantityordered > (select max_value from min_max_values)
	order by quantityordered desc
)

-- Update/Replace outliers with avarage value of QUANTITYORDERED
update sales_dataset_rfm_prj
set quantityordered = (select avg(quantityordered) from sales_dataset_rfm_prj)
where quantityordered in(select * from outliers);


-- Store cleaned values into a new table called SALES_DATASET_RFM_PRJ_CLEAN
create table SALES_DATASET_RFM_PRJ_CLEAN
as table sales_dataset_rfm_prj with no data;

insert into SALES_DATASET_RFM_PRJ_CLEAN
select *
from sales_dataset_rfm_prj;
	

-- Adding ContactLastName, ContactFirstName columns that are extracted from ContactFullName column
ALTER TABLE sales_dataset_rfm_prj
ADD contactfirstname VARCHAR,
ADD contactlastname VARCHAR ;

update sales_dataset_rfm_prj
set contactfirstname = initcap(left(contactfullname, position('-' in contactfullname) -1));

update sales_dataset_rfm_prj
set contactlastname = initcap(right(contactfullname, length(contactfullname) - position('-' in contactfullname)));


-- Adding QTR_ID, MONTH_ID, YEAR_ID as Quarter, Month, Year that are extracted from ORDERDATE column
ALTER TABLE sales_dataset_rfm_prj
ADD qtr_id integer,
ADD month_id integer,
ADD year_id integer; 

update sales_dataset_rfm_prj
set qtr_id = extract(quarter from orderdate);

update sales_dataset_rfm_prj
set month_id = extract(month from orderdate);

update sales_dataset_rfm_prj
set year_id = extract(year from orderdate)


-- Revenue for each ProductLine, Year and DealSize
select year_id, productline, dealsize,
		round(sum(sales), 2)
from public.sales_dataset_rfm_prj
group by year_id, productline, dealsize
order by year_id, productline;


-- Month with the highest sales
with max_sales as (
	select sum(sales) as sales
	from public.sales_dataset_rfm_prj
	group by month_id, ordernumber
	order by sum(sales) desc
	limit 1
)

select month_id, ordernumber, sum(sales) as revenue
from public.sales_dataset_rfm_prj
group by month_id, ordernumber
having sum(sales) = (select * from max_sales);


-- Most sold ProductLine in November
select ordernumber, 
		productline,
		sum(quantityordered) as total_quantity_ordered
from public.sales_dataset_rfm_prj
where month_id = 11
group by ordernumber, productline
order by sum(quantityordered) desc
limit 1;


-- Product has the highest sales in UK every year
select year_id, productline, 
		sum(sales) as revenue,
		rank() over (partition by year_id order by sum(sales) desc)
from public.sales_dataset_rfm_prj
where country like ('UK')
group by year_id, productline;


-- Best customer based on RFM Score
-- Calculate R-F-M values
with rfm as (
	
	select customername, 
			current_date - MAX(orderdate) as R,
			count(distinct ordernumber) as F,
			round(sum(sales), 2) as M
	from public.sales_dataset_rfm_prj
	group by customername
),

-- Categorize the values based on scale 1-5
rfm_score as (
	
	select customername, 
		ntile(5) over (order by R desc) as r_score,
		ntile(5) over (order by F) as f_score,
		ntile(5) over (order by M) as m_score
	from rfm
),
	
-- Categorize the values based on RFM scores
rfm_final as(
	select customername,
			concat(r_score, f_score, m_score) as rfm_score
	from rfm_score
),


-- Categorize customer into the appropriate RFM segmentation
rfm_segment as(
	select t1.customername, t2.segment
	from rfm_final t1
	join segment_score t2
	on t1.rfm_score = t2.scores
),


-- Select the customers with the best RFM scores, in 'Champions' segmentation
best_customer_rfm as(
	select *
	from rfm_segment
	where segment = 'Champions'
),


-- Count the number of customers in each segmentation to create a heat map accordingly
rfm_heatmap as(
	select segment, count(*)
	from rfm_segment
	group by segment
	order by count(*)
)

select *
from rfm_heatmap








		


	


