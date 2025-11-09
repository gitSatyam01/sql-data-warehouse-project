exec bronze.load_bronze;
Create or alter Procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime,@batch_end_time datetime
	begin try

	set @batch_start_time=GETDATE();
		print '================================================================================';
		print 'Loading bronz layer';
		print '================================================================================';


		print '--------------------------------------------------------------------------------';
		print 'Loading CRM table'
		print '--------------------------------------------------------------------------------';


		set @start_time=getdate();
		print '>>> Truncating table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>>> adding table: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\lilha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow=2,
		FIELDTERMINATOR=',',
		tablock
		);
		set @end_time=getdate();
		print '>>>Load Duration :'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'  seconds';
		print '----------';

		set @start_time=getdate();
		print '>>> Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>>> adding table: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\lilha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow=2,
		FIELDTERMINATOR=',',
		tablock
		);
		set @end_time=getdate();
		print '>>>Load Duration :'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'  seconds';
		print '----------';



		set @start_time=getdate();
		print '>>> Truncating table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>>> adding table: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\lilha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow=2,
		FIELDTERMINATOR=',',
		tablock
		);
        set @end_time=getdate();
		print '>>>Load Duration :'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'  seconds';
		print '----------';

		print '--------------------------------------------------------------------------------';
		print 'Loading ERP table'
		print '--------------------------------------------------------------------------------';

		set @start_time=getdate();
		print '>>> Truncating table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>>> adding table: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\lilha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
		firstrow=2,
		FIELDTERMINATOR=',',
		tablock
		);
		set @end_time=getdate();
		print '>>>Load Duration :'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'  seconds';
		print '----------';

		set @start_time=getdate();
		print '>>> Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>>> adding table: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\lilha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
		firstrow=2,
		FIELDTERMINATOR=',',
		tablock
		);
		set @end_time=getdate();
		print '>>>Load Duration :'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'  seconds';
		print '----------';
		set @start_time=getdate();
		print '>>> Truncating table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>>> adding table: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\lilha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
		firstrow=2,
		FIELDTERMINATOR=',',
		tablock
		);
		set @end_time=getdate();
		print '>>>Load Duration :'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'  seconds';
		print '----------';

		set @batch_end_time=GETDATE();
		print '>>> Total Load Duration :'+cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+'  seconds';
		print '***********************************************';

	end try
	begin catch
		print '========================================================'
		print 'Error occured during loading bronz layer'
		print 'Error Message'+error_message();
		print 'Error meggage'+cast(error_number() as nvarchar);
		print 'Error meggage'+cast(error_state() as nvarchar);
		print '========================================================'
	end catch
end
