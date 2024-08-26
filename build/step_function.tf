resource "aws_sfn_state_machine" "gdelt_step_function" {
  name     = "gdelt_step_function"
  role_arn = aws_iam_role.step_function_role.arn

  definition = <<JSON
{
  "Comment": "Step Function to execute Lambda and Glue processes",
  "StartAt": "IngestionFunction",
  "States": {
    "IngestionFunction": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${var.region}:${var.account_id}:function:${aws_lambda_function.ingestion_function.function_name}",
      "Next": "GlueJob"
    },
    "GlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${aws_glue_job.gdelt_glue_job.name}"
      },
      "Next": "LoadFunction"
    },
    "LoadFunction": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${var.region}:${var.account_id}:function:${aws_lambda_function.redshift_load.function_name}",
      "Next": "ParallelTrainingExecution"
    },
    "ParallelTrainingExecution": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "TrainingFunction",
          "States": {
            "TrainingFunction": {
              "Type": "Task",
              "Resource": "arn:aws:lambda:${var.region}:${var.account_id}:function:${aws_lambda_function.model_training.function_name}",
              "End": true
            }
          }
        },
        {
          "StartAt": "ExecutionFunction",
          "States": {
            "ExecutionFunction": {
              "Type": "Task",
              "Resource": "arn:aws:lambda:${var.region}:${var.account_id}:function:${aws_lambda_function.execution.function_name}",
              "End": true
            }
          }
        }
      ],
      "End": true
    }
  }
}
JSON
}

resource "aws_iam_role" "step_function_role" {
  name = "step_function_role"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [{
      "Action": "sts:AssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "states.${var.region}.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "step_function_policy" {
  name        = "step_function_policy"
  role        = aws_iam_role.step_function_role.id
  description = "Policy to allow Step Function to invoke Lambdas and Glue Jobs"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "lambda:InvokeFunction",
          "glue:StartJobRun"
        ],
        "Resource": "*"
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "daily_midnight" {
  name                = "DailyMidnight"
  description         = "Triggers Step Function at midnight UTC every day"
  schedule_expression = "cron(0 0 * * ? *)"
}

resource "aws_cloudwatch_event_target" "start_step_function" {
  rule      = aws_cloudwatch_event_rule.daily_midnight.name
  target_id = "StartStepFunction"
  arn       = aws_sfn_state_machine.gdelt_step_function.arn

  role_arn = aws_iam_role.step_function_role.arn
}

resource "aws_lambda_permission" "allow_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_midnight.arn
}
