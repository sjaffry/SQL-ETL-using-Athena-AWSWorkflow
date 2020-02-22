import sys
import boto3
import time

athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

workflowName = 'AthenaETLWorkflow'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
queryExecId = workflow_params['joinQueryExecutionId']

queryStatus = athena_client.get_query_execution(QueryExecutionId=queryExecId)['QueryExecution']['Status']['State']

while (queryStatus != 'SUCCEEDED'):
    queryStatus = athena_client.get_query_execution(QueryExecutionId=queryExecId)['QueryExecution']['Status']['State']
    if (queryStatus == 'FAILED' or queryStatus == 'CANCELLED'):
        raise NameError('Query execution failed')
    time.sleep(20)

print ('Query execution status is: ' + queryStatus)
