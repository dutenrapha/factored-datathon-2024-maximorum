python_requirements(
    name="reqs",
)

python_sources()


python_aws_lambda_function(
    name="ingestion_function", 
    handler="ingestion_function.py:lambda_handler",
    runtime="python3.9",
    include_requirements=True, 
    output_path="injection/lambda.zip"
)

python_aws_lambda_function(
    name="redshift_load", 
    handler="redshift_load.py:lambda_handler",
    runtime="python3.9",
    include_requirements=True, 
    output_path="load/lambda.zip"
)

python_aws_lambda_function(
    name="forecast_metrics", 
    handler="forecast_metrics.py:lambda_handler",
    runtime="python3.9",
    include_requirements=True, 
    output_path="forecast/lambda.zip"
)