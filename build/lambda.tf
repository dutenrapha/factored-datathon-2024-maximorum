
resource "aws_iam_role" "lambda_execution_role" {
  name = "lambda_execution_role"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      }
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "lambda_policy" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::339713000240:policy/AWSLambdaDatathon"
}

resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.ingestion_function.function_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "ingestion_function" {
  function_name = "ingestion_function"
  role          = aws_iam_role.lambda_execution_role.arn
  handler       = "lambda_function.handler"
  runtime       = "python3.9"

  filename      = "${path.module}/../dist/injection/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/injection/lambda.zip")

  environment {
    variables = {
      S3_BUCKET_NAME = var.S3_BUCKET_NAME
    }
  }

  timeout = 900
  memory_size = 3008
}


resource "aws_lambda_function" "redshift_load" {
  function_name = "redshift_load"
  role          = aws_iam_role.lambda_execution_role.arn
  handler       = "lambda_function.handler"
  runtime       = "python3.9"

  filename      = "${path.module}/../dist/load/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/load/lambda.zip")

  environment {
    variables = {
      S3_BUCKET_NAME = var.S3_BUCKET_NAME
    }
  }

  timeout = 900
  memory_size = 3008
}


resource "aws_lambda_function" "forecast_metrics" {
  function_name = "forecast_metrics"
  role          = aws_iam_role.lambda_execution_role.arn
  image_uri = "339713000240.dkr.ecr.us-east-2.amazonaws.com/gdelt:latest"
  package_type = "Image"
 

  environment {
    variables = {
      S3_BUCKET_NAME = var.S3_BUCKET_NAME
    }
  }

  timeout = 900
  memory_size = 3008
}
