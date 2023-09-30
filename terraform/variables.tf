variable "mock_project_roles"{
   type = set(string)
   default = ["SIG_ELT","SIG_DEVELOPER","SIG_PII"]
}

variable "mock_project_schemas"{
   type = set(string)
   default = ["RAW","CURATED","CONSUMPTION","DASHBOARD"]
}

variable "bucket_name" {
   default = "mock-project-bucket-sa"
}



variable "warehouse"{
   type = set(string)
   default = ["MOCK_PROJECT_WAREHOUSE","LOAD_WH","ADHOC_WH"]
}

variable "warehouse_size"{
   default = {
      "MOCK_PROJECT_WAREHOUSE" = "SMALL",
      "LOAD_WH" = "LARGE",
      "ADHOC_WH" = "SMALL"
   }
}

variable "AWS_ACCESS_KEY" {
   description = "AWS access key"
} 

variable "AWS_SECRET_KEY" {
   description = "AWS SECRET key"
} 
