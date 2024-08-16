python_requirements(
    name="reqs",
)

python_sources()


python_aws_lambda_function(
    name="ingestion_function", 
    handler="ingestion_function.py:lambda_handler",
    runtime="python3.9",
    include_requirements=True, 
    output_path="lambda.zip"
)