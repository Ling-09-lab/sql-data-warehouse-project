
/* CREATE PROCEDURE BRONZE.LOAD_BRONZE */
CREATE OR ALTER  PROCEDURE BRONZE.LOAD_BRONZE AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
	
	BEGIN TRY
		/*========================================
		TRUNCATE TABLES FIRST, THEN WRITE SQL BULK INSERT TO LOAD ALL CSV FILES INTO BRONZE TABLES.
		==========================================
		*/
		SET @BATCH_START_TIME = GETDATE();
		PRINT '>>>LOADING BRONZE LAYER';
		PRINT '----------LOADING CRM TABLES--------';
		SET @START_TIME = GETDATE();
		TRUNCATE TABLE [bronze].[crm_cust_info];
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS';
		PRINT '------------------------------';

		SET @START_TIME = GETDATE();
		TRUNCATE TABLE [bronze].[crm_prd_info];
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------';


		SET @START_TIME = GETDATE();
		TRUNCATE TABLE [bronze].[crm_sales_details];
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------';

		
		PRINT '----------LOADING ERP TABLES--------';
		SET @START_TIME = GETDATE();
		TRUNCATE TABLE [bronze].[erp_cust_az12];
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------';

		SET @START_TIME = GETDATE();
		TRUNCATE TABLE [bronze].[erp_loc_a101];
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------';

		SET @START_TIME = GETDATE();
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------';
		SET @BATCH_END_TIME = GETDATE();
		PRINT '>>>>>TOTAL LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@BATCH_START_TIME,@BATCH_END_TIME) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '======ERROR OCCURED DURING LOADING BRONZE LAYER=========';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
