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
    QueryString='''CREATE TABLE fhv_yellow_transformed
                WITH (
                    format = 'Parquet',
                    parquet_compression = 'SNAPPY',
                    external_location = 's3://'''+BUCKET+'''/fhv_yellow_transformed/')
                AS 
                select pickup_datetime as pickup_datetime, 
                concat(cast(rate_code as varchar),'-nyctaxi') as rate_code_converted,
                'NA' as dispatching_base_number
                FROM fhv_yellow;;''',
    QueryExecutionContext={
        'Database': DATABASE
    },
    ResultConfiguration={
        'OutputLocation': 's3://'+OUTPUT
    }
)

queryExecutionId = response['QueryExecutionId']

workflow_params['transformQueryExecutionId'] = queryExecutionId
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('Query execution id: ' + workflow_params['transformQueryExecutionId'])
