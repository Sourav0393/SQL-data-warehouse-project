/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
--EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze as
Begin
	
	Declare @batch_start_time datetime, @batch_end_time datetime, @start_time Datetime, @end_time Datetime;
	Begin Try
		set @batch_start_time = getdate();
		print '========================================================';
		print 'Loading the Bronze Layer';
		print '========================================================';
	
		print '--------------------------------------------------------';
		print 'Loading CRM Tables';
		print '--------------------------------------------------------';

		Set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_cust_info'
		truncate table bronze.crm_cust_info;

		print '>> Inserting Data into: bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info
		from 'E:\Data Analysis\SQL\Data Warehouse Project\source crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		Set @end_time = getdate();
		Print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

		set @start_time = getdate()
		print '>> Truncating Table: bronze.crm_prd_info'
		truncate table bronze.crm_prd_info;

		print '>> Inserting Data into: bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'E:\Data Analysis\SQL\Data Warehouse Project\source crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate()
		Print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

		set @start_time = getdate()
		print '>> Truncating Table: bronze.crm_sales_details'
		truncate table bronze.crm_sales_details;

		print '>> Inserting Data into: bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details
		from 'E:\Data Analysis\SQL\Data Warehouse Project\source crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate()
		Print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

		print '--------------------------------------------------------';
		print 'Loading ERP Tables';
		print '--------------------------------------------------------';

		set @start_time = getdate()
		print '>> Truncating Table: bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12;

		print '>> Inserting Data into: bronze.erp_cust_az12'
		bulk insert bronze.erp_cust_az12
		from 'E:\Data Analysis\SQL\Data Warehouse Project\source erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate()
		Print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		set @start_time = getdate()
		print '>> Truncating Table: bronze.erp_loc_a101'
		truncate table bronze.erp_loc_a101;

		print '>> Inserting Data into: bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from 'E:\Data Analysis\SQL\Data Warehouse Project\source erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate()
		Print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		
		set @start_time = getdate()
		print '>> Truncating Table: bronze.erp_px_cat_g1v2'
		truncate table bronze.erp_px_cat_g1v2;

		print '>> Inserting Data into: bronze.erp_px_cat_g1v2'
		bulk insert bronze.erp_px_cat_g1v2
		from 'E:\Data Analysis\SQL\Data Warehouse Project\source erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '>> ----------------';

		set @batch_end_time = getdate()
		print '======================================='
		print 'Loading Bronze Layer is Completed';
		print '>> Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
		print '======================================='

	End Try
	Begin Catch
		Print '========================================================='
		Print 'Error occured during Loading Bronze Layer'
		Print 'Error Message' + Error_message();
		Print 'Error Number' + cast(Error_number() as nvarchar);
		Print 'Error Message' + cast(Error_State() as nvarchar);
		Print '========================================================='
	End Catch
	
End
