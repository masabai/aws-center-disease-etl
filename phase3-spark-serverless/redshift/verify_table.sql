SELECT "table" AS table_name,
       tbl_rows AS row_count
FROM svv_table_info
WHERE schema = 'public'
  AND "table" IN ('cdc_chronic', 'cdc_heart');
