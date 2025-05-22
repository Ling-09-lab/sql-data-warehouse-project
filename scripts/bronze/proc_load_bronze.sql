/*=======================================================================
stored procedure:BRONZE.LOAD_BRONZE(load bronze layer)
script purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	-Truncates the bronze tables before loading data.
	-Use the 'Bulk insert' command to load data from csv files to bronze tables

Usage Example:
	EXEC bronze.load_bronze;
============================================================================
*/


CREATE OR ALTER PROCEDURE BRONZE.LOAD_BRONZE AS
BEGIN
	--LOADING DATA INTO SIX TABLES OF BRONZE LAYER
	print '=====LOADING BRONZE LAYER=====';
	DECLARE @START_TIME DATETIME,@END_TIME DATETIME;
	BEGIN TRY
		SET @START_TIME=GETDATE();
		print '=====LOADING BRONZE LAYER=====';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*) FROM bronze.crm_cust_info
		

		TRUNCATE TABLE [bronze].[crm_prd_info];
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(
			FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*) FROM [bronze].[crm_prd_info]

		TRUNCATE TABLE [bronze].[crm_sales_details];
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_crm\SALES_DETAILS.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SELECT COUNT(*) FROM [bronze].[crm_sales_details]

		TRUNCATE TABLE [bronze].[erp_cust_az12] ;
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_ERP\CUST_AZ12.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SELECT COUNT(*) FROM [bronze].[erp_cust_az12] 

		TRUNCATE TABLE [bronze].[erp_loc_a101];
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_ERP\loc_a101.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SELECT COUNT(*) FROM [bronze].[erp_loc_a101] 

		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\Xingj\OneDrive - Broward County Public Schools\Desktop\SQL\learning\baraa\sql-data-warehouse-project\datasets\source_ERP\px_cat_g1v2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SELECT COUNT(*) FROM [bronze].[erp_px_cat_g1v2] 
	END TRY
	BEGIN CATCH
		PRINT'=====ERROR OCCURED DURING LOADING BRONZE LAYER===='
		PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE'+CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
	SET @END_TIME=GETDATE();
		PRINT '>>LOAD DURATION:'+CAST( DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+'SECONDS';
END
