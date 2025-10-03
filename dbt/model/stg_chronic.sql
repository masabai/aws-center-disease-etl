select
    stratification1,
    stratificationid1,
    stratificationcategory1,
    stratificationcategoryid1
from {{ source('cdc','chronic_raw') }} -- sources not ready in airflow