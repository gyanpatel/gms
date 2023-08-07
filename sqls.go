package main

var MSSDefnSQL string = `select  col.name as column_name,
t.name as data_type 
from sys.tables as tab
inner join sys.columns as col
	   on tab.object_id = col.object_id
left join sys.types as t
on col.user_type_id = t.user_type_id
where tab.name = ?
order by column_name`

var PGDefnSQL string = `SELECT
a.attname as "Column",
pg_catalog.format_type(a.atttypid, a.atttypmod) as "Datatype"
FROM
pg_catalog.pg_attribute a
WHERE
a.attnum > 0
AND NOT a.attisdropped
AND a.attrelid = (
	SELECT c.oid
	FROM pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relname = $1
		AND pg_catalog.pg_table_is_visible(c.oid) 
) order by 1`
var ORADefnSQL string = `select COLUMN_NAME,DATA_TYPE from all_tab_columns WHERE owner = :1 AND TABLE_NAME =  :2 order by 1`
