-- Count of rows with NULLs in data_value column
SELECT COUNT(*) AS null_data_value_rows
FROM cdc_chronic
WHERE data_value IS NULL;

SELECT COUNT(*) AS null_data_value_rows
FROM cdc_heart
WHERE data_value IS NULL;
