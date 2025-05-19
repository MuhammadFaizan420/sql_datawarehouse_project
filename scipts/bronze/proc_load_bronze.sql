EXEC bronze.load_bronze;


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
	    SET @batch_start_time = getdate();
		  PRINT  '==========================================';
		  PRINT  ' Loading Bronze Layer'
		  PRINT  '==========================================';

		  PRINT  '-------------------------------------------';
		  PRINT  ' Loading CRM Tables'
		  PRINT  '-------------------------------------------';

		  SET @start_time = Getdate();
		  PRINT'>> Truncating Table :bronze.crm_cust_info'; 
	      TRUNCATE TABLE bronze.crm_cust_info;
		  PRINT'>> Inserting data into Table:bronze.crm_cust_info';
		  BULK INSERT bronze.crm_cust_info
		  FROM 'C:\Users\MuhammadFaizan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		  WITH
		      (
			 FIRSTROW = 2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		      );
           SET @end_time = GETDATE();
		   PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		   PRINT '--------------'

		  SET @start_time = Getdate();
		PRINT'>> Truncating Table:bronze.crm_prd_info'; 
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>> Inserting data into Table:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\MuhammadFaizan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
			 FIRSTROW = 2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		);
           SET @end_time = GETDATE();
		   PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		   PRINT '--------------'

		   SET @start_time = Getdate();
		PRINT'>> Truncating Table:bronze.crm_sales_details'; 
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>> Inserting data into Table:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\MuhammadFaizan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			 FIRSTROW = 2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		);
           SET @end_time = GETDATE();
		   PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		   PRINT '--------------'

		  PRINT  '-------------------------------------------';
		  PRINT  ' Loading ERP Tables';
		  PRINT  '-------------------------------------------';

		  SET @start_time = Getdate();
		PRINT'>> Truncating Table:bronze.erp_loc_a101'; 
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'>> Inserting data into Table:bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\MuhammadFaizan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			 FIRSTROW = 2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		);
           SET @end_time = GETDATE();
		   PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		   PRINT '--------------'

		   SET @start_time = Getdate();
		PRINT'>> Truncating Table:bronze.erp_cust_az12'; 
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'>> Inserting data into Table:bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\MuhammadFaizan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			 FIRSTROW = 2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		);
           SET @end_time = GETDATE();
		   PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		   PRINT '--------------'

		   SET @start_time = Getdate();
		PRINT'>> Truncating Table:bronze.erp_px_cat_g1v2'; 
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'>> Inserting data into Table:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\MuhammadFaizan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			 FIRSTROW = 2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		);
           SET @end_time = GETDATE();
		   PRINT '>> Load Duration '+ CAST(DATEDIFF(second, @start_time,@end_time) as VARCHAR) + ' seconds'
		   PRINT '--------------'

		   SET @batch_end_time = getdate();
		   PRINT'===============================';
		   PRINT'Loading Bronze Layer is Completed';
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
