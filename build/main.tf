provider "aws" {
  region = "us-east-2"
}

resource "aws_s3_bucket" "gdelt_project" {
  bucket = "gdelt-project"

  tags = {
    Name        = "GDELT Project"
    Environment = "Production"
  }
}

resource "aws_s3_object" "bronze_folder" {
  bucket = aws_s3_bucket.gdelt_project.bucket
  key    = "bronze/"
  acl    = "private"
}

resource "aws_s3_object" "silver_folder" {
  bucket = aws_s3_bucket.gdelt_project.bucket
  key    = "silver/"
  acl    = "private"
}

resource "aws_s3_object" "gold_folder" {
  bucket = aws_s3_bucket.gdelt_project.bucket
  key    = "gold/"
  acl    = "private"
}
