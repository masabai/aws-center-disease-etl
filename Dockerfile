FROM apache/airflow:2.7.0

# Switch to airflow user first
USER airflow

# Install Python dependencies for airflow user
RUN pip install --no-cache-dir \
    great_expectations \
    pyarrow==10.0.1
