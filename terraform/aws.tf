resource "aws_iam_role" "aws_s3_role" {
  name = "role_aws_s3_mock"
  
  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Principal": {
                "AWS": snowflake_storage_integration.s3_integration.storage_aws_iam_user_arn
            },
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": snowflake_storage_integration.s3_integration.storage_aws_external_id
                }
            }
        }
    ]
})
}


resource "snowflake_storage_integration" "s3_integration" {
  name                      = "MOCK_S3_STORAGE_INTEGRATION"
  comment                   = "Storage integration used to read files from S3 staging bucket"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = "arn:aws:iam::587298544823:role/role_aws_s3_mock"
  storage_allowed_locations = ["s3://s3-bucket-mock-sa/"]
}

resource "aws_s3_bucket" "s3_bucket" {
  bucket = "s3-bucket-mock-sa"
}

resource "aws_iam_policy" "s3_policy" {
  name        = "s3_mock-bucket-sql_access_policy"
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "s3-object-lambda:*"
            ],
            "Resource": "*"
        }
    ]
})
}

resource "aws_iam_policy_attachment" "s3_policy_attach" {
  name       = "s3_policy_attachment"
  roles      = [aws_iam_role.aws_s3_role.name]
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "snowflake_stage" "s3_external_stage" {
  name        = "MOCK_S3_EXTERNAL_STAGE"
  url         = snowflake_storage_integration.s3_integration.storage_allowed_locations[0]
  database    = snowflake_database.db.name
  schema      = "RAW"
  storage_integration = snowflake_storage_integration.s3_integration.name
   file_format = "FORMAT_NAME = 'MOCK_PROJECT_DB.RAW.PARQUET_FILE_FORMAT'"
}


