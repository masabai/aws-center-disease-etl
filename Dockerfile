# Uses the Airflow image as the base
FROM apache/airflow:2.7.0

# Switch to airflow user first
USER airflow

# Install Python dependencies for airflow user
RUN pip install --no-cache-dir \
    openai \
    great_expectations \
    pyarrow==10.0.1 \
    pytest \
