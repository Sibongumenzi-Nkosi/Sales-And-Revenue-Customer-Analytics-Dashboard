SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

-- Create database if it doesn't exist
IF DB_ID('NkosiDB') IS NULL
BEGIN
    CREATE DATABASE NkosiDB;
END
GO

USE NkosiDB;
GO


DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = STRING_AGG('ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(fk.parent_object_id)) 
                        + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';', CHAR(13) + CHAR(10))
FROM sys.foreign_keys fk
JOIN sys.tables t ON fk.referenced_object_id = t.object_id
WHERE t.name IN ('dim_date', 'dim_customer', 'dim_product');

IF @sql IS NOT NULL
BEGIN
    PRINT 'Dropping foreign key constraints:';
    PRINT @sql;
    EXEC sp_executesql @sql;
END
ELSE
BEGIN
    PRINT 'No foreign keys found referencing dimension tables.';
END
DROP TABLE IF EXISTS dbo.fact_sales;
DROP TABLE IF EXISTS dbo.dim_date;
DROP TABLE IF EXISTS dbo.dim_customer;
DROP TABLE IF EXISTS dbo.dim_product;
DROP TABLE IF EXISTS dbo.stg_sales;
DROP TABLE IF EXISTS dbo.stg_customers;
DROP TABLE IF EXISTS dbo.stg_products;

--------------------------------------------------------------------------------
-- 1) Staging tables
--------------------------------------------------------------------------------

-- Drop and create staging tables
IF OBJECT_ID('dbo.stg_sales') IS NOT NULL DROP TABLE dbo.stg_sales;
CREATE TABLE dbo.stg_sales (
    OrderID       VARCHAR(50)      NULL,
    OrderDate     VARCHAR(50)      NULL, 
    CustomerID    VARCHAR(50)      NULL,
    ProductID     VARCHAR(50)      NULL,
    Quantity      VARCHAR(50)      NULL, 
    UnitPrice     VARCHAR(50)      NULL,
    Channel       VARCHAR(100)     NULL,
    Region        VARCHAR(100)     NULL,
    LoadTS        DATETIME2        DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('dbo.stg_customers') IS NOT NULL DROP TABLE dbo.stg_customers;
CREATE TABLE dbo.stg_customers (
    CustomerID VARCHAR(50) NULL,
    Name       VARCHAR(200) NULL,
    JoinDate   VARCHAR(50) NULL,
    Region     VARCHAR(100) NULL,
    Segment    VARCHAR(100) NULL,
    LoadTS     DATETIME2 DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('dbo.stg_products') IS NOT NULL DROP TABLE dbo.stg_products;
CREATE TABLE dbo.stg_products (
    ProductID   VARCHAR(50) NULL,
    ProductName VARCHAR(200) NULL,
    Category    VARCHAR(100) NULL,
    LoadTS      DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

--------------------------------------------------------------------------------
-- 2) Dimensions
--------------------------------------------------------------------------------

-- Drop and create dimension tables
IF OBJECT_ID('dbo.dim_date') IS NOT NULL DROP TABLE dbo.dim_date;
CREATE TABLE dbo.dim_date (
    DateKey     INT          NOT NULL PRIMARY KEY,
    [Date]      DATE         NOT NULL,
    YearInt     INT          NOT NULL,
    MonthInt    INT          NOT NULL,
    MonthName   VARCHAR(20)  NOT NULL,
    QuarterInt  INT          NOT NULL,
    IsWeekend   BIT          NOT NULL,
    IsHoliday   BIT          NULL
);

IF OBJECT_ID('dbo.dim_customer') IS NOT NULL DROP TABLE dbo.dim_customer;
CREATE TABLE dbo.dim_customer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID  VARCHAR(50) NOT NULL UNIQUE,
    [Name]      VARCHAR(200) NULL,
    Region      VARCHAR(100) NULL,
    Segment     VARCHAR(100) NULL,
    JoinDate    DATE NULL,
    CurrentFlag BIT DEFAULT 1 NOT NULL,
    LoadTS      DATETIME2 DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('dbo.dim_product') IS NOT NULL DROP TABLE dbo.dim_product;
CREATE TABLE dbo.dim_product (
    ProductKey  INT IDENTITY(1,1) PRIMARY KEY,
    ProductID   VARCHAR(50) NOT NULL UNIQUE,
    ProductName VARCHAR(200) NULL,
    Category    VARCHAR(100) NULL,
    LoadTS      DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

--------------------------------------------------------------------------------
-- 3) Fact table with indexes
--------------------------------------------------------------------------------

IF OBJECT_ID('dbo.fact_sales') IS NOT NULL DROP TABLE dbo.fact_sales;
CREATE TABLE dbo.fact_sales (
    SaleKey     INT IDENTITY(1,1) PRIMARY KEY,
    OrderID     VARCHAR(50) NOT NULL,
    DateKey     INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey  INT NOT NULL,
    Quantity    INT NOT NULL,
    UnitPrice   DECIMAL(18,2) NOT NULL,
    Revenue     AS (Quantity * UnitPrice) PERSISTED,
    Channel     VARCHAR(100) NULL,
    Region      VARCHAR(100) NULL,
    LoadTS      DATETIME2 DEFAULT SYSUTCDATETIME()
);

-- Create indexes
CREATE INDEX IX_fact_sales_DateKey ON dbo.fact_sales(DateKey);
CREATE INDEX IX_fact_sales_CustomerKey ON dbo.fact_sales(CustomerKey);
CREATE INDEX IX_fact_sales_ProductKey ON dbo.fact_sales(ProductKey);
CREATE INDEX IX_fact_sales_OrderID ON dbo.fact_sales(OrderID);
CREATE INDEX IX_fact_sales_Date_Product ON dbo.fact_sales(DateKey, ProductKey) 
    INCLUDE (Quantity, UnitPrice, Revenue);
GO

--------------------------------------------------------------------------------
-- 4) Optimized Date Dimension Population
--------------------------------------------------------------------------------
PRINT 'Populating dim_date for 2018-01-01 through 2026-12-31 using set-based approach';

-- Set-based date population (much faster than WHILE loop)
INSERT INTO dbo.dim_date (DateKey, [Date], YearInt, MonthInt, MonthName, QuarterInt, IsWeekend)
SELECT 
    DateKey = CONVERT(INT, CONVERT(VARCHAR(8), d.[Date], 112)),
    d.[Date],
    YearInt = YEAR(d.[Date]),
    MonthInt = MONTH(d.[Date]),
    MonthName = DATENAME(MONTH, d.[Date]),
    QuarterInt = DATEPART(QUARTER, d.[Date]),
    IsWeekend = CASE WHEN DATEPART(WEEKDAY, d.[Date]) IN (1,7) THEN 1 ELSE 0 END
FROM (
    SELECT DATEADD(DAY, number, '2018-01-01') AS [Date]
    FROM master.dbo.spt_values
    WHERE type = 'P' 
    AND number <= DATEDIFF(DAY, '2018-01-01', '2026-12-31')
) d
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.dim_date dd 
    WHERE dd.DateKey = CONVERT(INT, CONVERT(VARCHAR(8), d.[Date], 112))
);

PRINT CONCAT('Inserted ', @@ROWCOUNT, ' dates into dim_date');
GO

--------------------------------------------------------------------------------
-- 5) Insert sample data into staging tables
--------------------------------------------------------------------------------
PRINT 'Loading sample data into staging tables';

TRUNCATE TABLE dbo.stg_sales;
TRUNCATE TABLE dbo.stg_customers;
TRUNCATE TABLE dbo.stg_products;

-- Customers
INSERT INTO dbo.stg_customers (CustomerID, Name, JoinDate, Region, Segment)
VALUES
 ('CUST-001', 'Alice Baker', '2020-03-15', 'North', 'Retail'),
 ('CUST-002', 'Bob Carter', '2019-11-22', 'East', 'Wholesale'),
 ('CUST-003', 'Carla Diaz', '2021-07-01', 'South', 'Retail');

-- Products
INSERT INTO dbo.stg_products (ProductID, ProductName, Category)
VALUES
 ('PROD-001', 'Acme Toothpaste', 'Personal Care'),
 ('PROD-002', 'Crunchy Cereal', 'Grocery'),
 ('PROD-003', 'Deluxe Shampoo', 'Personal Care');

-- Sales
INSERT INTO dbo.stg_sales (OrderID, OrderDate, CustomerID, ProductID, Quantity, UnitPrice, Channel, Region)
VALUES
 ('ORD-1001', '2021-01-05', 'CUST-001', 'PROD-001', '2', '3.50', 'Online', 'North'),
 ('ORD-1002', '2021-01-05', 'CUST-002', 'PROD-002', '1', '4.99', 'InStore', 'East'),
 ('ORD-1003', '2021-02-10', 'CUST-001', 'PROD-003', '3', '6.25', 'Online', 'North'),
 ('ORD-1004', '2021-02-15', 'CUST-003', 'PROD-002', '2', '4.99', 'InStore', 'South');

PRINT 'Sample data loaded into staging tables';
GO

--------------------------------------------------------------------------------
-- 6) Main ETL Process
--------------------------------------------------------------------------------
SET NOCOUNT ON;
GO

BEGIN TRY
    BEGIN TRANSACTION;
    PRINT 'Starting ETL process...';

    --------------------------------------------------------------------------------
    -- Create and populate temp table #clean_sales
    --------------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#clean_sales') IS NOT NULL DROP TABLE #clean_sales;

    CREATE TABLE #clean_sales (
        OrderID VARCHAR(50),
        OrderDate DATE,
        CustomerID VARCHAR(50),
        ProductID VARCHAR(50),
        Quantity INT,
        UnitPrice DECIMAL(18,2),
        Channel VARCHAR(100),
        Region VARCHAR(100),
        LoadTS DATETIME2,
        rn INT
    );

    PRINT 'Cleaning and preparing sales data...';
    
    ;WITH clean_sales_raw AS (
        SELECT
            OrderID = LTRIM(RTRIM(OrderID)),
            OrderDateRaw = LTRIM(RTRIM(OrderDate)),
            CustomerID = LTRIM(RTRIM(CustomerID)),
            ProductID = LTRIM(RTRIM(ProductID)),
            QuantityRaw = LTRIM(RTRIM(Quantity)),
            UnitPriceRaw = LTRIM(RTRIM(UnitPrice)),
            Channel = LTRIM(RTRIM(Channel)),
            Region = LTRIM(RTRIM(Region)),
            LoadTS
        FROM dbo.stg_sales
    ),
    clean_sales AS (
        SELECT
            OrderID,
            OrderDate = TRY_CAST(OrderDateRaw AS DATE),
            CustomerID,
            ProductID,
            Quantity = TRY_CAST(QuantityRaw AS INT),
            UnitPrice = TRY_CAST(UnitPriceRaw AS DECIMAL(18,2)),
            Channel,
            Region,
            LoadTS,
            rn = ROW_NUMBER() OVER (PARTITION BY OrderID ORDER BY LoadTS DESC)
        FROM clean_sales_raw
    )
    INSERT INTO #clean_sales
    SELECT * FROM clean_sales;

    PRINT CONCAT('Cleaned ', @@ROWCOUNT, ' sales records');
    
    --------------------------------------------------------------------------------
    -- 1) Upsert into dim_customer from staging customers
    --------------------------------------------------------------------------------
    PRINT 'Processing customer dimension...';
    
    MERGE dbo.dim_customer AS T
    USING (
        SELECT DISTINCT 
            CustomerID, 
            Name, 
            Region, 
            Segment, 
            TRY_CAST(JoinDate AS DATE) AS JoinDate
        FROM dbo.stg_customers
    ) AS S
    ON T.CustomerID = S.CustomerID
    WHEN MATCHED AND (
        ISNULL(T.Region, '') <> ISNULL(S.Region, '') OR
        ISNULL(T.Segment, '') <> ISNULL(S.Segment, '') OR
        ISNULL(T.Name, '') <> ISNULL(S.Name, '')
    )
    THEN UPDATE SET
        T.Region = S.Region,
        T.Segment = S.Segment,
        T.Name = S.Name,
        T.JoinDate = S.JoinDate,
        T.LoadTS = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (CustomerID, Name, Region, Segment, JoinDate)
         VALUES (S.CustomerID, S.Name, S.Region, S.Segment, S.JoinDate);

    PRINT CONCAT('Processed ', @@ROWCOUNT, ' customer records');
    
    --------------------------------------------------------------------------------
    -- 2) Upsert into dim_product from staging products
    --------------------------------------------------------------------------------
    PRINT 'Processing product dimension...';
    
    MERGE dbo.dim_product AS T
    USING (
        SELECT DISTINCT ProductID, ProductName, Category
        FROM dbo.stg_products
    ) AS S
    ON T.ProductID = S.ProductID
    WHEN MATCHED AND (
        ISNULL(T.ProductName, '') <> ISNULL(S.ProductName, '') OR
        ISNULL(T.Category, '') <> ISNULL(S.Category, '')
    )
    THEN UPDATE SET
        T.ProductName = S.ProductName,
        T.Category = S.Category,
        T.LoadTS = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (ProductID, ProductName, Category)
         VALUES (S.ProductID, S.ProductName, S.Category);

    PRINT CONCAT('Processed ', @@ROWCOUNT, ' product records');
    
    --------------------------------------------------------------------------------
    -- 3) Insert missing dates from clean_sales into dim_date
    --------------------------------------------------------------------------------
    PRINT 'Checking for missing dates...';
    
    ;WITH distinct_dates AS (
        SELECT DISTINCT 
            CONVERT(INT, CONVERT(VARCHAR(8), OrderDate, 112)) AS DateKey, 
            OrderDate
        FROM #clean_sales
        WHERE rn = 1 AND OrderDate IS NOT NULL
    )
    INSERT INTO dbo.dim_date (DateKey, [Date], YearInt, MonthInt, MonthName, QuarterInt, IsWeekend)
    SELECT 
        d.DateKey,
        d.OrderDate,
        DATEPART(YEAR, d.OrderDate),
        DATEPART(MONTH, d.OrderDate),
        DATENAME(MONTH, d.OrderDate),
        DATEPART(QUARTER, d.OrderDate),
        CASE WHEN DATEPART(WEEKDAY, d.OrderDate) IN (1,7) THEN 1 ELSE 0 END
    FROM distinct_dates d
    WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_date dd WHERE dd.DateKey = d.DateKey);

    PRINT CONCAT('Added ', @@ROWCOUNT, ' missing dates');
    
    --------------------------------------------------------------------------------
    -- 4) Insert new sales into fact_sales
    --------------------------------------------------------------------------------
    PRINT 'Loading fact sales...';
    
    INSERT INTO dbo.fact_sales (OrderID, DateKey, CustomerKey, ProductKey, Quantity, UnitPrice, Channel, Region)
    SELECT
        s.OrderID,
        CONVERT(INT, CONVERT(VARCHAR(8), s.OrderDate, 112)) AS DateKey,
        c.CustomerKey,
        p.ProductKey,
        ISNULL(s.Quantity, 0) AS Quantity,
        ISNULL(s.UnitPrice, 0.00) AS UnitPrice,
        s.Channel,
        s.Region
    FROM (
        SELECT * FROM #clean_sales WHERE rn = 1
    ) s
    INNER JOIN dbo.dim_customer c ON s.CustomerID = c.CustomerID
    INNER JOIN dbo.dim_product p ON s.ProductID = p.ProductID
    LEFT JOIN dbo.fact_sales f ON f.OrderID = s.OrderID
    WHERE f.OrderID IS NULL;

    PRINT CONCAT('Loaded ', @@ROWCOUNT, ' new sales records');
    
    --------------------------------------------------------------------------------
    -- Add foreign key constraints
    --------------------------------------------------------------------------------
    PRINT 'Adding foreign key constraints...';
    
    ALTER TABLE dbo.fact_sales ADD CONSTRAINT FK_fact_sales_dim_date
    FOREIGN KEY (DateKey) REFERENCES dbo.dim_date(DateKey);

    ALTER TABLE dbo.fact_sales ADD CONSTRAINT FK_fact_sales_dim_customer
    FOREIGN KEY (CustomerKey) REFERENCES dbo.dim_customer(CustomerKey);

    ALTER TABLE dbo.fact_sales ADD CONSTRAINT FK_fact_sales_dim_product
    FOREIGN KEY (ProductKey) REFERENCES dbo.dim_product(ProductKey);

    --------------------------------------------------------------------------------
    -- Validation queries
    --------------------------------------------------------------------------------
    PRINT 'Validating data loads...';
    
    SELECT 'stg_sales' AS Source, COUNT(*) AS Rows FROM dbo.stg_sales
    UNION ALL
    SELECT 'dim_customer' AS Source, COUNT(*) AS Rows FROM dbo.dim_customer
    UNION ALL
    SELECT 'dim_product' AS Source, COUNT(*) AS Rows FROM dbo.dim_product
    UNION ALL
    SELECT 'fact_sales' AS Source, COUNT(*) AS Rows FROM dbo.fact_sales
    UNION ALL
    SELECT 'dim_date' AS Source, COUNT(*) AS Rows FROM dbo.dim_date;

    COMMIT TRANSACTION;
    PRINT 'ETL process completed successfully';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    DECLARE @err NVARCHAR(MAX) = 
        'Error in ' + ERROR_PROCEDURE() + 
        ' (Line ' + CAST(ERROR_LINE() AS VARCHAR) + '): ' + 
        ERROR_MESSAGE();
    
    PRINT 'ETL failed with error:';
    PRINT @err;

    -- Re-throw the error to surface it to the calling application
    THROW; 
END CATCH;
GO

PRINT 'Data warehouse setup and ETL process complete'


