FROM public.ecr.aws/lambda/python:3.9

# Install Great Expectations and dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy validation script and GX config
COPY validate_nutri_container.py ${LAMBDA_TASK_ROOT}/
COPY great_expectations/ ${LAMBDA_TASK_ROOT}/great_expectations/

CMD ["validate_nutri_container.lambda_handler"]
