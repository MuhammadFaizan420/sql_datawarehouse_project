
--/// Creating GOLD.DIM_CUSTOMER DATA MART
CREATE OR ALTER VIEW gold.dim_customer
AS

SELECT
      ROW_NUMBER() Over(order by cst_id) as customer_key 
      ,ci.cst_id as customer_id
      ,ci.cst_key as customer_number
      ,ci.cst_firstname as first_name
      ,ci.cst_lastname as last_name
	  ,cl.cntry as country
      ,ci.cst_marital_status as marital_status
	  ,CASE when ci.cst_gender != 'n/a' then ci.cst_gender
	         else coalesce(ce.gen,'na/')
			 end as gender
      ,ce.bdate as birth_date   
      ,ci.cst_create_date as create_date
  FROM [Datawarehouse].[silver].[crm_cust_info] ci
  left join silver.erp_cust_az12 ce on ci.cst_key=ce.cid
  left join silver.erp_loc_a101 cl on ci.cst_key = cl.cid

--///////  CREATING GOLD._DIM_PRODUCTS DATA MART
CREATE OR ALTER VIEW gold.dim_products 
AS
select
    ROW_NUMBER() over (order by cp.prd_strt_dt,cp.prd_key) as product_key,
	cp.prd_id as product_id,
	cp.prd_key as product_number,
	cp.prd_nm as product_name,
	cp.cat_id as category_id,
	ep.cat as category,
	ep.subcat as sub_category,
	ep.maintenance,
	cp.prd_cost as product_cost,
	cp.prd_line as product_line,
	cp.prd_strt_dt as start_date
from silver.crm_prd_info cp
LEFT JOIN silver.erp_px_cat_g1v2 ep on cp.cat_id=ep.id
where cp.prd_end_dt is NULL

--//// CREATING GOLD.FACT_SALES DATA MART
CREATE OR ALTER VIEW gold.fact_sales
AS
select 
sls_ord_nm as order_number,
p.product_key,
c.customer_key,
sls_ord_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_sales as sales_amount,
sls_quantity as quantity,
sls_price as price
from silver.crm_sales_details s
 LEFT JOIN gold.dim_products p on s.sls_prd_key = p.product_number
 LEFT JOIN gold.dim_customer c on s.sls_cust_id = c.customer_id
