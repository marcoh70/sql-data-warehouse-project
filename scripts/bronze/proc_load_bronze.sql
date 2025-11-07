--- LOAD DATA
EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '==================================================================='
		PRINT 'Loading bronze layer'
		PRINT 'Loading CRM Tables'
		PRINT '==================================================================='

		PRINT '>> Truncating table bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data into: bronze.crm_cust_info'
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Marko\Marko\unir\data-warehouse-udemy-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		--SELECT COUNT(*) FROM bronze.crm_cust_info;
		SET @end_time = GETDATE();
		PRINT '>> Loading data into crm_cust_info duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '-------------------------------------------------------------------------------------'
		
		PRINT '>> Truncating table bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into: bronze.crm_prd_info'
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Marko\Marko\unir\data-warehouse-udemy-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		--SELECT * FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Loading data into crm_prd_info duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '-------------------------------------------------------------------------------------'
		

		PRINT '>> Truncating table bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into: bronze.crm_sales_details'
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Marko\Marko\unir\data-warehouse-udemy-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Loading data into crm_sales_details duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '-------------------------------------------------------------------------------------'

		/*SELECT column_name, data_type, ordinal_position
		from information_schema.columns
		where table_name = 'crm_sales_details'
		and table_schema = 'bronze'
		order by ordinal_position;*/

		PRINT '==================================================================='
		PRINT 'Loading ERP Tables'
		PRINT '==================================================================='

		PRINT '>> Truncating table bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into table bronze.erp_cust_az12'
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Marko\Marko\unir\data-warehouse-udemy-course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Loading data into erp_cust_az12 duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '-------------------------------------------------------------------------------------'

		PRINT '>> Truncating table bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into table bronze.erp_loc_a101'
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Marko\Marko\unir\data-warehouse-udemy-course\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Loading data into erp_loc_a101 duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '-------------------------------------------------------------------------------------'

		PRINT '>> Truncating table bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into table bronze.erp_px_cat_g1v2'
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Marko\Marko\unir\data-warehouse-udemy-course\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Loading data into erp_px_cat_g1v2 duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '-------------------------------------------------------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT '======================================================'
		PRINT 'Error ocurrs during loading bronze layer'
		PRINT 'Error message'+ ERROR_MESSAGE();
		PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR); 
		PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================'
	END CATCH
END


----------------------------------------
