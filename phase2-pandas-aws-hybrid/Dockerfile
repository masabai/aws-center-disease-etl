FROM public.ecr.aws/lambda/python:3.9

# Install Great Expectations and dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy validation script
COPY lambdas/validate_cdc/validate_nutri_container.py ${LAMBDA_TASK_ROOT}/

CMD ["validate_nutri_container.lambda_handler"]
