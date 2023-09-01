from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor 
from airflow.operators.bash import BashOperator
from Parquet import convert_parquet
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pytz
import os
import re


# Converting the execution time to a specified format for slack notifications
def convert_datetime(datetime_string):
    return datetime_string.astimezone(pytz.timezone('America/Denver')).strftime('%b-%d %H:%M:%S')


# Configuring the slack connection id
SLACK_CONN_ID = 'slack_conn'

def task_fail_slack_alert(context):
    
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    slack_msg = f"""
        ❌ Task Failed. Check the logs to identify what went wrong.
        *Task*: {context['task_instance'].task_id}
        *Dag*: {context['task_instance'].dag_id}
        *Execution Time*: {convert_datetime(context['execution_date'])  }
        <{context['task_instance'].log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)

SNOWFLAKE_CONN_ID = "snowflake_conn_mock"
SNOWFLAKE_TABLE_1 = "MOCK_PROJECT_DB.RAW.ORDER_SUMMARY"
SNOWFLAKE_TABLE_2 = "MOCK_PROJECT_DB.RAW.LINE_ITEM_SUPPLY"
SNOWFLAKE_TABLE_3 = "MOCK_PROJECT_DB.RAW.PART_SUPPLY_SUPPLIER"

# USE_DATABASE =f"USE DATABASE MOCK_PROJECT_DB"
# USE_SCHEMA =f"USE SCHEMA RAW"

COPY_FROM_EXTERNAL_STAGE_ORDER_SUMMARY =  f"""
COPY INTO {SNOWFLAKE_TABLE_1}
(CUSTKEY,CUSTOMER,CUST_ADDRESS,PHONE,CUST_ACCTBAL,CUST_MKTSEGMENT,ORDERKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,SHIPPRIORITY,
ORDER_COMMENT,NATIONKEY,NATION,REGIONKEY,REGION)
  FROM (
  SELECT 
  $1:CUSTKEY::NUMBER(38,0) AS CUSTKEY,
    $1:CUSTOMER::VARCHAR(100) AS CUSTOMER,
    $1:CUST_ADDRESS::VARCHAR(200) AS CUST_ADDRESS,
    $1:PHONE::VARCHAR(20) AS PHONE,
    $1:CUST_ACCTBAL::NUMBER(10,4) AS CUST_ACCTBAL,
    $1:CUST_MKTSEGMENT::VARCHAR(20) AS CUST_MKTSEGMENT,
    $1:ORDERKEY::NUMBER(15,0) AS ORDERKEY,
    $1:ORDERSTATUS::VARCHAR(10) AS ORDERSTATUS,
    $1:TOTALPRICE::NUMBER(10,4) AS TOTALPRICE,
    $1:ORDERDATE::VARCHAR(10) AS ORDERDATE,
    $1:ORDERPRIORITY::VARCHAR(50) AS ORDERPRIORITY,
    $1:CLERK::VARCHAR(50) AS CLERK,
    $1:SHIPPRIORITY::VARCHAR(20) AS SHIPPRIORITY,
    $1:ORDER_COMMENT::VARCHAR(255) AS ORDER_COMMENT,
    $1:NATIONKEY::NUMBER(20,0) AS NATIONKEY,
    $1:NATION::VARCHAR(30) AS NATION,
    $1:REGIONKEY::NUMBER(20,0) AS REGIONKEY,
    $1:REGION::VARCHAR(20) AS REGION
FROM @mock_s3_external_stage/parquet_files/Order_summary/ 
)
file_format = parquet_file_format
on_error = 'continue' 
"""

COPY_FROM_EXTERNAL_STAGE_LINE_ITEM = f"""
COPY INTO {SNOWFLAKE_TABLE_2}
(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,LINE_ITEM_COMMENT)
  FROM (
    SELECT 
    $1:ORDERKEY::NUMBER(38,0) AS ORDERKEY,
    $1:PARTKEY::NUMBER(38,0) AS PARTKEY,
    $1:SUPPKEY::NUMBER(38,0) AS SUPPKEY,
    $1:LINENUMBER::NUMBER(5,0) AS LINENUMBER,
    $1:QUANTITY::NUMBER(10,4) AS QUANTITY,
    $1:EXTENDEDPRICE::NUMBER(10,4) AS EXTENDEDPRICE,
    $1:DISCOUNT::NUMBER(5,4) AS DISCOUNT,
    $1:TAX::NUMBER(5,4) AS TAX,
    $1:RETURNFLAG::VARCHAR(10) AS RETURNFLAG,
    $1:LINESTATUS::VARCHAR(10) AS LINESTATUS,
    $1:SHIPDATE::VARCHAR(10) AS SHIPDATE,
    $1:COMMITDATE::VARCHAR(10) AS COMMITDATE,
    $1:RECEIPTDATE::VARCHAR(10) AS RECEIPTDATE,
    $1:SHIPINSTRUCT::VARCHAR(100) AS SHIPINSTRUCT,
    $1:SHIPMODE::VARCHAR(20) AS SHIPMODE,
    $1:LINE_ITEM_COMMENT::VARCHAR(255) AS LINE_ITEM_COMMENT
    FROM @mock_s3_external_stage/parquet_files/line_item/ 
)
file_format = parquet_file_format
on_error = 'continue'
"""

COPY_FROM_EXTERNAL_STAGE_PART_SUPPLY = f"""
 COPY INTO {SNOWFLAKE_TABLE_3}
 (PARTKEY,PART_NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER,RETAILPRICE,PART_COMMENT,SUPPKEY,AVAILQTY,SUPPLYCOST,PART_SUPPLIER_COMMENT,SUPPLIER_NAME,ADDRESS,SUPPLIER_NATION,PHONE,ACCTBAL,SUPPLIER_COMMENT)
  FROM (
SELECT 
    $1:PARTKEY::NUMBER(38,0) AS PARTKEY,
    $1:PART_NAME::VARCHAR(200) AS PART_NAME,
    $1:MFGR::VARCHAR(50) AS MFGR,
    $1:BRAND::VARCHAR(30) AS BRAND,
    $1:TYPE::VARCHAR(100) AS TYPE,
    $1:SIZE::NUMBER(10,0) AS SIZE,
    $1:CONTAINER::VARCHAR(30) AS CONTAINER,
    $1:RETAILPRICE::NUMBER(10,4) AS RETAILPRICE,
    $1:PART_COMMENT::VARCHAR(255) AS PART_COMMENT,
    $1:SUPPKEY::NUMBER(38,0) AS SUPPKEY,
    $1:AVAILQTY::NUMBER(10,0) AS AVAILQTY,
    $1:SUPPLYCOST::NUMBER(10,4) AS SUPPLYCOST,
    $1:PART_SUPPLIER_COMMENT::VARCHAR(255) AS PART_SUPPLIER_COMMENT,
    $1:SUPPLIER_NAME::VARCHAR(100) AS SUPPLIER_NAME,
    $1:ADDRESS::VARCHAR(200) AS ADDRESS,
    $1:SUPPLIER_NATION::NUMBER(20,0) AS SUPPLIER_NATION,
    $1:PHONE::VARCHAR(20) AS PHONE,
    $1:ACCTBAL::NUMBER(10,4) AS ACCTBAL,
    $1:SUPPLIER_COMMENT::VARCHAR(255) AS SUPPLIER_COMMENT
FROM @mock_s3_external_stage/parquet_files/part_supply/ 
)
file_format = parquet_file_format
on_error = 'continue'
"""

def upload_to_s3(parq_files: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')

    for dir, subdir, files in os.walk(parq_files):
        for file in files:
            file_name = file
            pos_digit = re.search(r"\d", file_name)
            length = pos_digit.start() - 1
            print(file)
            hook.load_file(filename=f"{parq_files}{file}", key=f"parquet_files/{file[:length]}/{file}", bucket_name=bucket_name)

default_args = {
    'owner': 'admin',
    'snowflake_conn_id': SNOWFLAKE_CONN_ID,
    'email': ["dflag44@gmail.com"],
    'email_on_failure': True,
    'on_failure_callback': task_fail_slack_alert
}

rootpath="/opt/airflow/csv_files"
with DAG(
    default_args=default_args,
    dag_id='ETL_DAG',
    description='Dag with python operators',
    start_date=datetime(2023,3,21),
    schedule='@daily',
    catchup=False
) as dag:
    
    dummy_task1 = DummyOperator(task_id="Start")
    task1 = FileSensor(
        task_id="check_for_files",
        filepath="/opt/airflow/csv_files/*.csv",
        fs_conn_id= "fs_default",
        poke_interval = 10,
        timeout = 150,
        soft_fail = True
    )
    task2 = PythonOperator(
        task_id = 'convert_csv_to_parquet',
        python_callable= convert_parquet,
        op_kwargs={'path':rootpath}
    )

    # Upload the file
    task3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'parq_files': '/opt/airflow/parquet_files/',
            'bucket_name': 's3-bucket-mock-sa'
        }
    )

    task4 = BashOperator(
        task_id = "Move_Processed_files_and_delete_parquet",
        bash_command = "mv /opt/airflow/csv_files/*.csv /opt/airflow/Processed_files; rm -rf /opt/airflow/parquet_files/*.parquet"
    )
  
    task5 =  SnowflakeOperator( task_id = "copy_from_external_stage_order_summary", sql=COPY_FROM_EXTERNAL_STAGE_ORDER_SUMMARY)
    task6 =  SnowflakeOperator( task_id = "copy_from_external_stage_line_item", sql=COPY_FROM_EXTERNAL_STAGE_LINE_ITEM)
    task7 =  SnowflakeOperator( task_id = "copy_from_external_stage_part_supply", sql=COPY_FROM_EXTERNAL_STAGE_PART_SUPPLY)

    task8 = BashOperator(task_id = "raw_test" , bash_command = "dbt test --select source:source_raw --profiles-dir /opt/airflow/.dbt --project-dir /opt/airflow/dbt_mock")
    task9 = BashOperator(task_id = "curated_run",bash_command = "dbt run --models curated --profiles-dir /opt/airflow/.dbt --project-dir /opt/airflow/dbt_mock")
    task10 = BashOperator(task_id = "curated_test" , bash_command = "dbt test --models curated --profiles-dir /opt/airflow/.dbt --project-dir /opt/airflow/dbt_mock")
    task11 = BashOperator(task_id = "consumption_run" , bash_command = "dbt run --models consumption dashboard --profiles-dir /opt/airflow/.dbt --project-dir /opt/airflow/dbt_mock")
    dummy_task2 = DummyOperator(task_id="End")
    
    dummy_task1 >> task1 >> task2 >> task3 >> task4 >> [task5,task6,task7] >> task8 >> task9 >> task10 >> task11 >> dummy_task2
