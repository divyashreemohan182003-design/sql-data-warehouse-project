/*
========================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=========================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze schema.
Actions Performed:
   -Truncates Silver tables.
   -Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
   None.
   This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC Silver.load_silver;
===========================================================================================
/*

create or alter procedure silver.load_silver as 
BEGIN 
   DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
   BEGIN TRY
     SET @batch_start_time = GETDATE();
     PRINT'============================================';
     PRINT'Loading Silver Layer';
     PRINT'============================================';

     PRINT'---------------------------------------------';
     print'Loading CRM Tables';
     PRINT'---------------------------------------------';
     
     --loading silver.crm_cust_info 
    SET @start_time = GETDATE();
    print '>> truncating table : silver.crm_cust_info';
    truncate table silver.crm_cust_info;
    print '>> inserting data into : silver.crm_cust_info';
    insert into silver.crm_cust_info (
        cst_id ,
        cst_key ,
        cst_firstname ,
        cst_lastname ,
        cst_marital_status ,
        cst_gndr ,
        cst_create_date)

    select
    cst_id,
    cst_key,
    trim (cst_firstname) as cst_firstname,
    trim (cst_lastname) as cst_lastname,
    case when upper(trim(cst_marital_status)) = 'S' THEN 'Single'
         when upper(trim(cst_marital_status)) = 'M' THEN 'Married'
         else 'n\a'
    end cst_marital_status,
    case when upper(trim(cst_gndr)) = 'F' THEN 'Female'
         when upper(trim(cst_gndr)) = 'M' THEN 'Male'
         else 'n\a'
    end cst_gndr,
    cst_create_date
    from(
    select
    *,
    ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
    from bronze.crm_cust_info
    where cst_id is not null
    )t where flag_last = 1
    set @end_time = getdate()
    print '>> load duration; ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
    print '>> -------------';

    --loading silver.crm_prd_info
    set @start_time = GETDATE()
    print '>> truncating table : silver.crm_prd_info';
    truncate table silver.crm_prd_info;
    print '>> inserting data into : silver.crm_prd_info';
    insert into silver.crm_prd_info(
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
    )
    select
    prd_id,
    replace (SUBSTRING(prd_key,1,5), '-', '_') as cat_id,
    substring(prd_key,7,len(prd_key)) as prd_key,
    prd_nm,
    isnull(prd_cost,0) as prd_cost,
    case when upper(trim(prd_line)) = 'M' THEN  'Mountain'
         when upper(trim(prd_line)) = 'R' THEN  'Road'
         when upper(trim(prd_line)) = 'S' THEN  'other sales'
         when upper(trim(prd_line)) = 'T' THEN  'Touring'
         ELSE 'n\a'
    end as prd_line,
    cast(prd_start_dt as date) as prd_start_dt,
    cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt_test
    from bronze.crm_prd_info
    set @end_time = getdate()
    print '>> load duration; ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
    print '>> -------------';
   
   
   
    --loading silver.crm_sales_details
    set @start_time = GETDATE()
    print '>> truncating table : silver.crm_sales_details';
    truncate table silver.crm_sales_details;
    print '>> inserting data into : silver.crm_sales_details';
    insert into silver.crm_sales_details(
        sls_ord_num ,
         sls_prd_key,
         sls_cust_id,
         sls_order_dt,
         sls_ship_dt,
         sls_due_dt,
         sls_sales,
         sls_quantity,
         sls_price 
    )
    select
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or len(sls_order_dt)!=8 then null
         else cast(cast(sls_order_dt as varchar)as date) 
    end AS sls_order_dt,
    case when sls_ship_dt = 0 or len(sls_ship_dt)!=8 then null
         else cast(cast(sls_ship_dt as varchar)as date) 
    end AS sls_ship_dt,
    case when sls_due_dt = 0 or len(sls_due_dt)!=8 then null
         else cast(cast(sls_due_dt as varchar)as date) 
    end AS sls_due_dt,
    case when sls_sales is null or sls_sales <= 0 or sls_sales!= sls_quantity * abs( sls_price)
         then sls_quantity * abs( sls_price) 
         else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_sales <= 0
         then sls_sales / nullif(sls_quantity,0)
         else abs(sls_price)
    end as sls_price
    from bronze.crm_sales_details
    set @end_time = getdate()
    print '>> load duration; ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
    print '>> -------------';
   
     PRINT'---------------------------------------------';
     print'Loading ERP Tables';
     PRINT'---------------------------------------------';

    --loading silver.erp_cust_az12
    set @start_time = GETDATE() 
    print '>> truncating table : silver.erp_cust_az12';
    truncate table silver.erp_cust_az12;
    print '>> inserting data into : silver.erp_cust_az12';
    insert into silver.erp_cust_az12(cid,bdate,gen)
    select
    case when cid like 'NAS%' then substring(cid, 4,len(cid))
          else cid
    end cid,
    case when bdate > getdate() then null
         else bdate
    end as bdate,
    case when gen is NULL or gen = ' ' then 'n\a'
         when gen = 'M' then 'Male'
         when gen = 'F' then 'Female'
         else gen
    end as gen
    from bronze.erp_cust_az12
    set @end_time = getdate()
    print '>> load duration; ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
    print '>> -------------';
    
    
    
    
    
    --loading silver.erp_loc_a101
    set @start_time = GETDATE()
    print '>> truncating table : silver.erp_loc_a101';
    truncate table silver.erp_loc_a101;
    print '>> inserting data into : silver.erp_loc_a101';
    insert into silver.erp_loc_a101(cid,cntry)
    select 
    replace(cid,'-','') cid,
    case when cntry is null or cntry = ' ' then 'n/a'
         when trim(cntry ) = 'DE' then 'Germany'
         when trim(cntry) in('US','USA') then 'United States'
         else TRIM(cntry)
    end as cntry
    from bronze.erp_loc_a101
    set @end_time = getdate()
    print '>> load duration; ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
    print '>> -------------';
   
   
   --loading silver.erp_px_cat_g1v2
    set @start_time = GETDATE() 
    print '>> truncating table : silver.erp_px_cat_g1v2';
    truncate table silver.erp_px_cat_g1v2;
    print '>> inserting data into : silver.erp_px_cat_g1v2';
    insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
    select
    id,
    cat,
    subcat,
    maintenance
    from bronze.erp_px_cat_g1v2
    set @end_time = getdate()
    print '>> load duration; ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
    print '>> -------------';

    SET @batch_end_time = GETDATE();
    PRINT '====================================='
    PRINT 'Loading Silver Layer is completed';
    print '   - total load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
    print '====================================='

 end try
 begin catch
     print '====================================='
     print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
     PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
     PRINT 'ERROR MESSAGE' + CAST( ERROR_NUMBER() AS NVARCHAR);
     PRINT 'ERROR MESSAGE' + CAST( ERROR_STATE() AS NVARCHAR);
     print '======================================'
 END CATCH
end
