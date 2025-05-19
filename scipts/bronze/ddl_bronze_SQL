--// ddl for crm tables
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
   DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
       cst_id int,
	   cst_key varchar(50),
	   cst_firstname varchar(50),
	   cst_lastname varchar(50),
	   cst_marital_status varchar(10),
	   cst_gender varchar(10),
	   cst_create_date Date
);

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
       prd_id int,
	   prd_key varchar(100),
	   prd_nm varchar(200),
	   prd_cost int,
	   prd_line varchar(10),
	   prd_strt_dt Date,
	   prd_end_dt Date
);

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details
(
       sls_ord_nm varchar(50),
	   sls_prd_key varchar(50),
	   sls_cust_id int,
	   sls_ord_dt int,
	   sls_ship_dt int,
	   sls_due_dt int,
	   sls_sales int,
	   sls_quantity int,
	   sls_price int
);

--// ddl for erp tables
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
        cid varchar(50),
		cntry varchar(50)
);

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
   DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
        cid varchar(50),
		bdate date,
		gen varchar(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2
(
         id varchar(50),
		 cat varchar(50),
		 subcat varchar(50),
		 maintenance varchar(50)
);

