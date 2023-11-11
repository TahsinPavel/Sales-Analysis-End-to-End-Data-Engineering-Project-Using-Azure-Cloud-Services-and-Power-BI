SELECT 
s.name AS SchemaName, 
t.name AS TableName 
From sys.tables as t 
INNER JOIN sys.schemas as s
ON t.schema_id = s.schema_id
WHERE s.name = 'SalesLT'