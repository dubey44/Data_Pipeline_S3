terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.59"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "snowflake" {
  role  = "SIG_DEPLOY"
#   account = env_var.'TF_VAR_SNOWFLAKE_ACCOUNT'
#   username = env_var.'TF_VAR_SNOWFLAKE_USERNAME'
#   region = env_var.'TF_VAR_SNOWFLAKE_REGION'
#   password = env_var.'TF_VAR_SNOWFLAKE_PASSWORD'
}

# Configure the AWS Provider
provider "aws" {
  region = "us-east-1"
  access_key = var.AWS_ACCESS_KEY
  secret_key = var.AWS_SECRET_KEY
}

