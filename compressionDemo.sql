--Estimate Space for Savings

USE ColumnstoreDemo
GO
EXEC sp_estimate_data_compression_savings 'DBO', 'BigTransaction', NULL,
    NULL, 'ROW';

EXEC sp_estimate_data_compression_savings 'DBO', 'BigTransaction', NULL,
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
ALTER TABLE [dbo].[bigTransactionHistory] REBUILD PARTITION = ALL
WITH 
(DATA_COMPRESSION = PAGE
);

ALTER INDEX [compression_index] ON [dbo].[bigTransactionHistory] 
REBUILD PARTITION = ALL WITH 
(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, 
ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DATA_COMPRESSION = PAGE);

ALTER TABLE [dbo].[bigTransaction_row] REBUILD PARTITION = ALL
WITH 
(DATA_COMPRESSION = ROW
);

ALTER INDEX [compression_index_row] ON [dbo].[bigTransactionHistory_row] 
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
        AND a.object_id = b.object_id;

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

SELECT  productid ,
        transactiondate ,
        quantity ,
        actualcost
FROM    bigtransaction
WHERE   Quantity > 70
        AND Quantity < 92
        AND TransactionDate > '2008-01-01';

--Show and Capture Statistics

EXEC dbo.show_buffers;


--Run Same Query Compressed

SELECT  TransactionID ,
        productid ,
        transactiondate ,
        quantity ,
        actualcost
FROM    dbo.bigtransaction_page
WHERE   Quantity > 70
        AND Quantity < 92
        AND TransactionDate > '2008-01-01';

--Capture Statistics and Buffers

EXEC dbo.show_buffers;


--Show Cost of Update (Single Row)

UPDATE  dbo.bigTransaction
SET     Quantity = 89
WHERE   TransactionID = 24018460;

UPDATE  dbo.bigTransaction_row
SET     Quantity = 89
WHERE   TransactionID = 24018460;

UPDATE  dbo.bigtransaction
SET     Quantity = 89
WHERE   TransactionID = 24018460;


--Show Cost of Update (Bulk)

--Page Compressed


UPDATE  dbo.bigtransaction_page
SET     Quantity = 92
WHERE   TransactionDate > '2008-01-01'
        AND TransactionDate < '2008-01-14';

--Row Compressed 

UPDATE  dbo.bigTransaction_row
SET     Quantity = 92
WHERE   TransactionDate > '2008-01-01'
        AND TransactionDate < '2008-01-14';

--Uncompressed

UPDATE  dbo.bigtransaction
SET     Quantity = 92
WHERE   TransactionDate > '2008-01-01'
        AND TransactionDate < '2008-01-14';

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

INSERT INTO cs_load_demo SELECT TOP 102399 * FROM dbo.bigtransaction_cs; 

INSERT INTO cs_load_demo SELECT TOP 1024000 * FROM dbo.bigtransaction_cs; 

SELECT * FROM sys.column_store_row_groups WHERE object_id=885578193



--Show query performance without Columnstore Index
SELECT transactiondate
	,avg(quantity)
	,avg(actualcost)
FROM bigtransaction
WHERE TransactionDate < '2007-07-01'
	AND Quantity > 70
	AND Quantity < 92
GROUP BY TransactionDate
ORDER BY transactionDate;

SELECT * FROM dbo.bigtransaction_cs


--Show query performance with Columnstore Index
SELECT transactiondate
	,avg(quantity)
	,avg(actualcost)
FROM dbo.bigtransaction_cs
WHERE TransactionDate < '2007-07-01'
	AND Quantity > 70
	AND Quantity < 92
GROUP BY TransactionDate
ORDER BY transactionDate;

EXEC dbo.show_buffers
EXEC dbo.show_CS_Buffer
