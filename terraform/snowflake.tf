resource "snowflake_database" "db" {
  name     = "MOCK_PROJECT_DB"
}

resource "snowflake_schema" "schema" {    
  database = snowflake_database.db.name
  for_each = var.mock_project_schemas
  name     = each.value
}

resource "snowflake_warehouse" "warehouse" {
  name           = each.value
  for_each = var.warehouse
  warehouse_size = var.warehouse_size[each.value]
  initially_suspended = true
  auto_suspend = 60
  auto_resume = true
} 


resource "snowflake_role" "role" {
  name    = each.value
  for_each = var.mock_project_roles
}

resource "snowflake_role_grants" "grants" {
  for_each = var.mock_project_roles
  role_name = each.value
  roles = [
    "SIG_DEPLOY"
  ]
  depends_on =[snowflake_role.role]
}

resource "snowflake_file_format" "file_format" {
  name        = "PARQUET_FILE_FORMAT"
  database    = snowflake_database.db.name
  schema      = "RAW"
  format_type = "PARQUET"
  compression = "snappy"
  depends_on = [snowflake_schema.schema]
}

resource "snowflake_table" "raw_table_1" {
  database            = snowflake_database.db.name
  schema              = "RAW"
  name                = "ORDER_SUMMARY"
  comment             = "This is a RAW Order_Summary Table"
  depends_on = [snowflake_schema.schema]

  column {
    name     = "CUSTKEY"
    type     = "NUMBER(38,0)"
  }

  column {
    name     = "CUSTOMER"
    type     = "VARCHAR(100)"
  }

  column {
    name     = "CUST_ADDRESS"
    type     = "VARCHAR(200)"
  }

  column {
    name = "PHONE"
    type = "VARCHAR(20)"
  }

  column {
    name    = "CUST_ACCTBAL"
    type    = "NUMBER(10,4)"
  }

   column {
    name    = "CUST_MKTSEGMENT"
    type    = "VARCHAR(20)"
  }

   column {
    name    = "ORDERKEY"
    type    = "NUMBER(15,0)"
  }

  column {
    name    = "ORDERSTATUS"
    type    = "VARCHAR(10)"
  }
   column {
    name    = "TOTALPRICE"
    type    = "NUMBER(10,4)"
  }
   column {
    name    = "ORDERDATE"
    type    = "VARCHAR(10)"
  }
   column {
    name    = "ORDERPRIORITY"
    type    = "VARCHAR(50)"
  }
   column {
    name    = "CLERK"
    type    = "VARCHAR(50)"
  }

    column {
    name    = "SHIPPRIORITY"
    type    = "VARCHAR(20)"
  }

    column {
    name    = "ORDER_COMMENT"
    type    = "VARCHAR(255)"
  }

  column {
    name    = "NATIONKEY"
    type    = "NUMBER(20,0)"
  }
    column {
    name    = "NATION"
    type    = "VARCHAR(30)"
  }

   column {
    name    = "REGIONKEY"
    type    = "NUMBER(20,0)"
  }

  column {
    name    = "REGION"
    type    = "VARCHAR(30)"
  }

  column {
    name    = "ETL_TS"
    type    = "timestamp"
    
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

resource "snowflake_table" "raw_table_2" {
  database            = snowflake_database.db.name
  schema              = "RAW"
  name                = "LINE_ITEM_SUPPLY"
  comment             = "This is a RAW Line_Item_Supply Table"
  depends_on = [snowflake_schema.schema]

  column {
    name     = "ORDERKEY"
    type     = "NUMBER(38,0)"
  }

  column {
    name     = "PARTKEY"
    type     = "NUMBER(38,0)"
  }

  column {
    name     = "SUPPKEY"
    type     = "NUMBER(38,0)"
  }

  column {
    name = "LINENUMBER"
    type = "NUMBER(5,0)"
  }

   column {
    name    = "QUANTITY"
    type    = "NUMBER(10,4)"
  }

   column {
    name    = "EXTENDEDPRICE"
    type    = "NUMBER(10,4)"
  }

  column {
    name    = "DISCOUNT"
    type    = "NUMBER(5,4)"
  }
   column {
    name    = "TAX"
    type    = "NUMBER(5,4)"
  }
   column {
    name    = "RETURNFLAG"
    type    = "VARCHAR(10)"
  }
   column {
    name    = "LINESTATUS"
    type    = "VARCHAR(10)"
  }
   column {
    name    = "SHIPDATE"
    type    = "VARCHAR(10)"
  }

    column {
    name    = "COMMITDATE"
    type    = "VARCHAR(10)"
  }

    column {
    name    = "RECEIPTDATE"
    type    = "VARCHAR(10)"
  }

  column {
    name    = "SHIPINSTRUCT"
    type    = "VARCHAR(100)"
  }
    column {
    name    = "SHIPMODE"
    type    = "VARCHAR(20)"
  }

   column {
    name    = "LINE_ITEM_COMMENT"
    type    = "VARCHAR(255)"
  }
   column {
    name    = "ETL_TS"
    type    = "timestamp"

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

resource "snowflake_table" "raw_table_3" {
  database            = snowflake_database.db.name
  schema              = "RAW"
  name                = "PART_SUPPLY_SUPPLIER"
  comment             = "This is a RAW Part_Supply_Supplier Table"
  depends_on = [snowflake_schema.schema]
  
  column {
    name     = "PARTKEY"
    type     = "NUMBER(38,0)"
  }

  column {
    name     = "PART_NAME"
    type     = "VARCHAR(200)"
  }

  column {
    name     = "MFGR"
    type     = "VARCHAR(50)"
  }

  column {
    name = "BRAND"
    type = "VARCHAR(30)"
  }

   column {
    name    = "TYPE"
    type    = "VARCHAR(100)"
  }

   column {
    name    = "SIZE"
    type    = "NUMBER(10,0)"
  }

  column {
    name    = "CONTAINER"
    type    = "VARCHAR(30)"
  }
   column {
    name    = "RETAILPRICE"
    type    = "NUMBER(10,4)"
  }
   column {
    name    = "PART_COMMENT"
    type    = "VARCHAR(255)"
  }
   column {
    name    = "SUPPKEY"
    type    = "NUMBER(38,0)"
  }
   column {
    name    = "AVAILQTY"
    type    = "NUMBER(10,0)"
  }

  column {
    name    = "SUPPLYCOST"
    type    = "NUMBER(10,4)"
  }

  column {
    name    = "PART_SUPPLIER_COMMENT"
    type    = "VARCHAR(255)"
  }

  column {
    name    = "SUPPLIER_NAME"
    type    = "VARCHAR(100)"
  }
    column {
    name    = "ADDRESS"
    type    = "VARCHAR(200)"
  }

   column {
    name    = "SUPPLIER_NATION"
    type    = "NUMBER(20,0)"
  }
   column {
    name    = "PHONE"
    type    = "VARCHAR(20)"
  }
   column {
    name    = "ACCTBAL"
    type    = "NUMBER(10,4)"
  }
   column {
    name    = "SUPPLIER_COMMENT"
    type    = "VARCHAR(255)"
  }

  column {
    name    = "ETL_TS"
    type    = "timestamp"
    
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}



