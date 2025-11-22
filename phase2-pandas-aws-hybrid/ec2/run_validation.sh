#!/bin/bash
# Activate virtualenv and run GX script
source /home/ec2-user/venv/bin/activate
python3 /home/ec2-user/cdc_pandas_etl/scripts/validate_nutri.py
