select distinct
    stratificationid1 as race_ethnicity_id,
    case
        when stratification1 = 'Overall' then 'Overall'
        when stratification1 like '%Hispanic%' and stratification1 not like '%non-Hispanic%'
            then null
        else split_part(stratification1, ',', 1)
    end as race,
    case
        when stratification1 = 'Overall' then null
        when stratification1 like '%non-Hispanic%' then 'non-Hispanic'
        when stratification1 like '%Hispanic%' then 'Hispanic'
        else null
    end as ethnicity,
    stratificationcategory1 as category
from {{ ref('stg_chronic') }}
