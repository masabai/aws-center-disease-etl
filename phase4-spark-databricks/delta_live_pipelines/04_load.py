import dlt

@dlt.table(
    name="cdc_gold_chronic",
    comment="Gold table for chronic data"
)
def load_chronic_gold():
    """
    Load cleaned chronic data from Silver layer into Gold layer.

    Returns:
        DataFrame: Gold-level chronic dataset.
    """
    # Read Silver table for chronic
    return dlt.read("cdc_silver_chronic")


@dlt.table(
    name="cdc_gold_heart",
    comment="Gold table for heart data"
)
def load_heart_gold():
    """
    Load cleaned heart data from Silver layer into Gold layer.

    Returns:
        DataFrame: Gold-level heart dataset.
    """
    # Read Silver table for heart
    return dlt.read("cdc_silver_heart")


@dlt.table(
    name="cdc_gold_nutri",
    comment="Gold table for nutrition data"
)
def load_nutri_gold():
    """
    Load cleaned nutrition data from Silver layer into Gold layer.

    Returns:
        DataFrame: Gold-level nutrition dataset.
    """
    # Read Silver table for nutrition
    return dlt.read("cdc_silver_nutri")
