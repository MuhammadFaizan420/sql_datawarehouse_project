--///// Loading Tables in Silver layer from bronze layer------------


EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
     Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	 BEGIN TRY
		 SET @batch_start_time = GETDATE()

		 PRINT '========Loading Silver Layer========'

		 PRINT '========Loading CRM Tables========'

		 SET @start_time = GETDATE()
		PRINT '>> Truncating Table :silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting data into :silver.crm_cust_info';

		Insert into silver.crm_cust_info
		(  cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
			)

		select 
				cst_id,
				cst_key,
				Trim(cst_firstname) as cst_firstname,
				Trim(cst_lastname) as cst_lastname,
				CASE
					when Upper(Trim(cst_marital_status)) = 'S' then 'Single'
					when Upper(Trim(cst_marital_status)) = 'M' then 'Married'
					else 'n/a'
				end as cst_marital_status,
				CASE 
					when Upper(trim(cst_gender)) = 'F' then 'Female'
					when Upper(Trim(cst_gender)) = 'M' then 'Male'
					else 'n/a'
				end as cst_gender	,
				cst_create_date
		from
		(
		select *,
		ROW_Number() over(partition by cst_id order by cst_create_date desc) as new_rank

		from bronze.crm_cust_info
		where cst_id is not null
		)t
		where new_rank =1;
		SET @end_time = getdate();
		PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		PRINT '--------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table :silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting data into :silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_strt_dt,
				prd_end_dt
		)
		select 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		COALESCE(prd_cost,0) as prd_cost,
		CASE 
			when upper(Trim(prd_line)) = 'M' then 'Mountain'
			when upper(Trim(prd_line)) = 'R' then 'Road'
			when upper(Trim(prd_line)) = 'S' then 'Other Sales'
			when upper(Trim(prd_line)) = 'T' then 'Touring'
			else 'n/a'
		end as prd_line,
		prd_strt_dt,
		DATEADD(day,-1,LEAD(prd_strt_dt) over(partition by prd_key order by prd_strt_dt))as prd_end_dt
		from bronze.crm_prd_info;
		SET @end_time = getdate();
		PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		PRINT '--------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details
		(     sls_ord_nm,
				sls_prd_key,
				sls_cust_id,
				sls_ord_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
		)

		SELECT  [sls_ord_nm]
				,[sls_prd_key]
				,[sls_cust_id]
				,CASE when [sls_ord_dt] = 0 or LEN(sls_ord_dt)!=8 then NULL
					else cast(cast(sls_ord_dt as Varchar) as Date)
				end as sls_ord_dt
				,CASE when [sls_ship_dt] = 0 or LEN(sls_ship_dt)!=8 then NULL
					else cast(cast(sls_ship_dt as Varchar) as Date)
				end as sls_ship_dt
				,CASE when [sls_due_dt] = 0 or LEN(sls_due_dt)!=8 then NULL
					else cast(cast(sls_due_dt as Varchar) as Date)
				end as sls_due_dt
				,CASE when sls_sales IS NULL or sls_sales<=0 OR sls_sales != sls_quantity*ABS(sls_price)
					then sls_quantity*ABS(sls_price)
					else sls_sales
				end as sls_sales
				,[sls_quantity]
				,CASE when sls_price IS NULL or sls_price<=0 
					then sls_sales/NULLIF(sls_quantity,0)
					else sls_price
				end as sls_price
			FROM [Datawarehouse].[bronze].[crm_sales_details];
			SET @end_time = getdate();
		PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		PRINT '--------------'

		PRINT '========Loading ERP Tables========'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12
		(      cid,
				bdate,
				gen
		)	   
		SELECT Case 
					when cid like '%NAS%' then SUBSTRING(cid,4,LEN(cid))
					else cid 
				end as cid
				,case 
					when bdate>getdate() then Null
					else bdate
				end as bdate
				,case 
					when Upper(Trim(gen)) in('F', 'Female') then 'Female'
					when Upper(Trim(gen)) in('M', 'Male') then 'Male'
					else 'n/a'
				end as gen
			FROM [bronze].[erp_cust_az12];
			SET @end_time = getdate();
		PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		PRINT '--------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		select 
				TRIM(REPLACE(cid, '-','')) as cid
				,case
					when TRIM(cntry) IN('US','USA') then 'United States'
					when TRIM(cntry) IN('DE') then 'Germany'
					when TRIM(cntry) is null or Trim(cntry) ='' then 'n/a'
					else cntry
				end as cntry	  
		from [bronze].[erp_loc_a101];
		SET @end_time = getdate();
		PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		PRINT '--------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
		(       id,cat,subcat,maintenance
		)
		select * from bronze.erp_px_cat_g1v2;
		SET @end_time = getdate();
		PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		PRINT '--------------';

		SET @batch_end_time = getdate();
		   PRINT'===============================';
		   PRINT'Loading Silver Layer is Completed';
		   PRINT' - Total Load Duration: ' + cast(datediff(second,@batch_start_time,@batch_end_time) as varchar) +' second';
		   PRINT'===============================';
        END TRY
		   BEGIN CATCH
				PRINT '================================='
				PRINT ' Error Occured During loading Bronze Layer'
				PRINT 'Error Message' + ERROR_MESSAGE();
				PRINT 'Error Number' + CAST(ERROR_NUMBER() as Varchar);
			END CATCH
END
