import dlt

# Gold table for Chronic Data
@dlt.table(
    name="cdc_gold_chronic",
    comment="Gold table for chronic data"
)
def load_chronic_gold():
    return dlt.read("cdc_silver_chronic")

# Gold table for Heart Data
@dlt.table(
    name="cdc_gold_heart",
    comment="Gold table for heart data"
)
def load_heart_gold():
    return dlt.read("cdc_silver_heart")

# Gold table for Nutrition Data
@dlt.table(
    name="cdc_gold_nutri",
    comment="Gold table for nutrition data"
)
def load_nutri_gold():
    return dlt.read("cdc_silver_nutri")
