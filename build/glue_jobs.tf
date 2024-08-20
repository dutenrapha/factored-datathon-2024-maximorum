resource "aws_iam_role" "glue_service_role" {
  name = "glue_service_role"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [{
      "Action": "sts:AssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy" "glue_policy" {
  name        = "GlueS3AccessPolicy"
  description = "Policy for Glue to access all S3 buckets"
  
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "s3:*",
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policy_attachment" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_policy.arn
}

resource "null_resource" "upload_unzip_script" {
  provisioner "local-exec" {
    command = "aws s3 cp ../src/unzip_job.py s3://${aws_s3_bucket.gdelt_project.bucket}/dependencies/unzip_job.py"
  }

  depends_on = [
    aws_s3_bucket.gdelt_project
  ]
}

resource "aws_glue_job" "gdelt_glue_job" {
  name        = "unzip_job"
  role_arn    = aws_iam_role.glue_service_role.arn
  command {
    script_location = "s3://${aws_s3_bucket.gdelt_project.bucket}/dependencies/unzip_job.py"
    name            = "unzip_job"
  }
  

  default_arguments = {
    "--TempDir"                = "s3://${aws_s3_bucket.gdelt_project.bucket}/temp/"
    "--job-bookmark-option"    = "job-bookmark-enable"
    "--enable-glue-datacatalog" = "true"
  }
  max_capacity = 3.0

  depends_on = [
    null_resource.upload_unzip_script
  ]
}
