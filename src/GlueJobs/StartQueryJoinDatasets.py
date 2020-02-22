import boto3
import json
import os

athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

workflowName = 'AthenaETLWorkflow'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
DATABASE = workflow_params['DATABASE_NAME']
# The S3 bucket\folder\ location where you would like query results saved.
OUTPUT = workflow_params['OUTPUT_LOCATION']
BUCKET = workflow_params['PROCESSED_BUCKET']

response = athena_client.start_query_execution(
    QueryString='''CREATE TABLE fhv_yellow
                WITH (
                    format = 'Parquet',
                    parquet_compression = 'SNAPPY',
                    external_location = 's3://'''+BUCKET+'''/fhv_yellow/')
                AS 
                SELECT "yellow".pickup_datetime, 
                "yellow".rate_code,
                "fhv".dispatching_base_number
                FROM "nyctaxi-data-db"."yellow" as "yellow", "nyctaxi-data-db"."fhv" as "fhv"
                WHERE "yellow".pickup_datetime = "fhv".pickup_datetime;''',
    QueryExecutionContext={
        'Database': DATABASE
    },
    ResultConfiguration={
        'OutputLocation': 's3://'+OUTPUT
    }
)

queryExecutionId = response['QueryExecutionId']

workflow_params['joinQueryExecutionId'] = queryExecutionId
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('Query execution id: ' + workflow_params['joinQueryExecutionId'])
