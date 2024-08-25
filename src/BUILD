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
)


docker_image(
    name="forecast_metrics_image",
    dependencies = [":forecast_metrics"],
)


python_aws_lambda_function(
    name="model_training", 
    handler="model_training.py:lambda_handler",
    runtime="python3.9",
    include_requirements=True, 
)


docker_image(
    name="model_training_image",
    dependencies = [":model_training"],
)


python_aws_lambda_function(
    name="execution", 
    handler="execution.py:lambda_handler",
    runtime="python3.9",
    include_requirements=True, 
)


docker_image(
    name="execution_image",
    dependencies = [":execution"],
)

python_aws_lambda_function(
    name="distance", 
    handler="distance.py:lambda_handler",
    runtime="python3.9",
    include_requirements=True, 
)


docker_image(
    name="distance_image",
    dependencies = [":distance"],
)