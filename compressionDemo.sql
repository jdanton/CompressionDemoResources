--Estimate Space for Savings

/*SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FactResellerSalesXL_Heap](
	[ProductKey] [int] NOT NULL,
	[OrderDateKey] [int] NOT NULL,
	[DueDateKey] [int] NOT NULL,
	[ShipDateKey] [int] NOT NULL,
	[ResellerKey] [int] NOT NULL,
	[EmployeeKey] [int] NOT NULL,
	[PromotionKey] [int] NOT NULL,
	[CurrencyKey] [int] NOT NULL,
	[SalesTerritoryKey] [int] NOT NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [tinyint] NOT NULL,
	[RevisionNumber] [tinyint] NULL,
	[OrderQuantity] [smallint] NULL,
	[UnitPrice] [money] NULL,
	[ExtendedAmount] [money] NULL,
	[UnitPriceDiscountPct] [float] NULL,
	[DiscountAmount] [float] NULL,
	[ProductStandardCost] [money] NULL,
	[TotalProductCost] [money] NULL,
	[SalesAmount] [money] NULL,
	[TaxAmt] [money] NULL,
	[Freight] [money] NULL,
	[CarrierTrackingNumber] [nvarchar](25) NULL,
	[CustomerPONumber] [nvarchar](25) NULL,
	[OrderDate] [datetime] NULL,
	[DueDate] [datetime] NULL,
	[ShipDate] [datetime] NULL
) ON [PRIMARY]
GO
*/

USE ColumnstoreDemo
GO
EXEC sp_estimate_data_compression_savings 'DBO', 'FactResellerSalesXL_Heap', NULL,
    NULL, 'ROW';

EXEC sp_estimate_data_compression_savings 'DBO', 'FactResellerSalesXL_Heap', NULL,
    NULL, 'PAGE';


--Checks for Percentage of Update Statements

SELECT  o.name AS [Table_Name] ,
        x.name AS [Index_Name] ,
        i.partition_number AS [Partition] ,
        i.index_id AS [Index_ID] ,
        x.type_desc AS [Index_Type] ,
        i.leaf_update_count * 100.0 / ( i.range_scan_count
                                        + i.leaf_insert_count
                                        + i.leaf_delete_count
                                        + i.leaf_update_count
                                        + i.leaf_page_merge_count
                                        + i.singleton_lookup_count ) AS [Percent_Update]
FROM    sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.indexes x ON x.object_id = i.object_id
                              AND x.index_id = i.index_id
WHERE   ( i.range_scan_count + i.leaf_insert_count + i.leaf_delete_count
          + leaf_update_count + i.leaf_page_merge_count
          + i.singleton_lookup_count ) != 0
        AND OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
ORDER BY [Percent_Update] DESC;

--Checks for Percentage of Scans

SELECT  o.name AS [Table_Name] ,
        x.name AS [Index_Name] ,
        i.partition_number AS [Partition] ,
        i.index_id AS [Index_ID] ,
        x.type_desc AS [Index_Type] ,
        i.range_scan_count * 100.0 / ( i.range_scan_count
                                       + i.leaf_insert_count
                                       + i.leaf_delete_count
                                       + i.leaf_update_count
                                       + i.leaf_page_merge_count
                                       + i.singleton_lookup_count ) AS [Percent_Scan]
FROM    sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.indexes x ON x.object_id = i.object_id
                              AND x.index_id = i.index_id
WHERE   ( i.range_scan_count + i.leaf_insert_count + i.leaf_delete_count
          + leaf_update_count + i.leaf_page_merge_count
          + i.singleton_lookup_count ) != 0
        AND OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
ORDER BY [Percent_Scan] DESC


--TurnOn Compression
/* 
ALTER TABLE [dbo].[FactResellerSalesXL_HeapHistory] REBUILD PARTITION = ALL
WITH 
(DATA_COMPRESSION = PAGE
);

ALTER INDEX [compression_index] ON [dbo].[FactResellerSalesXL_HeapHistory] 
REBUILD PARTITION = ALL WITH 
(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, 
ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DATA_COMPRESSION = PAGE);

ALTER TABLE [dbo].[FactResellerSalesXL_Heap_row] REBUILD PARTITION = ALL
WITH 
(DATA_COMPRESSION = ROW
);

ALTER INDEX [compression_index_row] ON [dbo].[FactResellerSalesXL_HeapHistory_row] 
REBUILD PARTITION = ALL WITH 
(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, 
ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DATA_COMPRESSION = ROW);

*/


--Show Compression

SELECT  b.name ,
        a.object_id ,
        a.data_compression,
		A.data_compression_desc
FROM    sys.partitions a ,
        sys.objects b
WHERE   a.data_compression <> 0
        AND a.object_id = b.object_id
        AND object_name(a.object_id) like '%FactResellerSalesXL%';

/* Indicates the state of compression for each partition:
0 = NONE
1 = ROW
2 = PAGE
3 = COLUMNSTORE 
4 = COLUMNSTORE ARCHIVAL*/

--Show Pages

select * from sys.dm_db_database_page_allocations(db_id(),object_id('bigProduct'),0,null,'DETAILED');

dbcc traceon (3604)

--Uncompressed Page
dbcc page ('ColumnstoreDemo',1,143628,2)

--Compressed Page
dbcc page ('ColumnstoreDemo',1,511510,2)

--Set stats on

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

--Run Initial Query 

SELECT  frs.orderdate,
        p.EnglishProductName,
        avg(frs.SalesAmount) AS [Avg_Sales_Amount] 
FROM    FactResellerSalesXL_Heap FRS
        JOIN DimProduct P ON FRS.ProductKey = P.ProductKey
GROUP BY frs.orderdate,
        p.EnglishProductName
ORDER BY AVS_Sales_Amount DESC;

--Show and Capture Statistics

EXEC dbo.show_buffers;


--Run Same Query Compressed


SELECT  frs.orderdate,
        p.EnglishProductName,
        avg(frs.SalesAmount) AS [Avg_Sales_Amount] 
FROM    FactResellerSalesXL_PageCompressed FRS
        JOIN DimProduct P ON FRS.ProductKey = P.ProductKey
GROUP BY frs.orderdate,
        p.EnglishProductName
ORDER BY AVS_Sales_Amount DESC;

--Capture Statistics and Buffers

EXEC dbo.show_buffers;


--Show Cost of Update (Single Row)

UPDATE  dbo.FactResellerSalesXL_Heap
SET     UnitPrice = 27.293
WHERE   SalesOrderNumber = 'SO45736';

UPDATE  dbo.FactResellerSalesXL_Heap_row
SET     UnitPrice = 27.293
WHERE   SalesOrderNumber = 'SO45736';

UPDATE  dbo.FactResellerSalesXL_Heap
SET     UnitPrice = 27.293
WHERE   SalesOrderNumber = 'SO45736';


--Show Cost of Update (Bulk)

--Page Compressed

BEGIN TRANSACTION

UPDATE  dbo.FactResellerSalesXL_Page
SET     UnitPrice = 27.293

ROLLBACK TRANSACTION    

--Columnstore Compressed 

BEGIN TRANSACTION

UPDATE  dbo.FactResellerSalesXL_CCI
SET     UnitPrice = 27.293

ROLLBACK TRANSACTION    


--Uncompressed
BEGIN TRANSACTION

UPDATE  dbo.FactResellerSalesXL_Heap
SET     UnitPrice = 27.293

ROLLBACK TRANSACTION

--Inline Compression Demos

CREATE TABLE People (
 _id int primary key identity,
 name nvarchar(max),
 surname nvarchar(max),
 info varbinary(max)
)

INSERT INTO People (name, surname, info)
 SELECT FirstName, LastName, COMPRESS(AdditionalContactInfo) FROM AdventureWorks2016CTP3.Person.Person

 SELECT name, surname, DECOMPRESS(info) AS original
 FROM People
--As an alternative, we can add computed column (non-persisted) that dynamically decompress data:

ALTER TABLE People
 ADD info_text as CAST( DECOMPRESS(info) AS NVARCHAR(MAX))


--Columnstore Demos

--Show Small Load versus Large Load 

TRUNCATE TABLE cs_load_demo;

INSERT INTO cs_load_demo SELECT TOP 102399 * FROM dbo.FactResellerSalesXL_Heap_cs; 

INSERT INTO cs_load_demo SELECT TOP 1024000 * FROM dbo.FactResellerSalesXL_Heap_cs; 

SELECT * FROM sys.column_store_row_groups WHERE object_id=885578193



--Show query performance without Columnstore Index
SELECT transactiondate
	,avg(quantity)
	,avg(actualcost)
FROM FactResellerSalesXL_Heap
WHERE TransactionDate < '2007-07-01'
	AND Quantity > 70
	AND Quantity < 92
GROUP BY TransactionDate
ORDER BY transactionDate;

SELECT * FROM dbo.FactResellerSalesXL_Heap_cs


--Show query performance with Columnstore Index
SELECT transactiondate
	,avg(quantity)
	,avg(actualcost)
FROM dbo.FactResellerSalesXL_Heap_cs
WHERE TransactionDate < '2007-07-01'
	AND Quantity > 70
	AND Quantity < 92
GROUP BY TransactionDate
ORDER BY transactionDate;

EXEC dbo.show_buffers
EXEC dbo.show_CS_Buffer
