-- Table size for CDC tables
SELECT "table" AS table_name,
       size AS size_in_mb
FROM svv_table_info
WHERE schema = 'public'
  AND "table" IN ('cdc_chronic', 'cdc_heart');
