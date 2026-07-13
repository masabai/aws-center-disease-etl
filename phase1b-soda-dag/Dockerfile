FROM apache/airflow:2.7.0

USER airflow

RUN pip install --no-cache-dir \
    "email-validator>=2.0" \
    soda-core==3.3.22 \
    soda-core-pandas-dask==3.3.22 \
    pyarrow==10.0.1 \
    pytest
