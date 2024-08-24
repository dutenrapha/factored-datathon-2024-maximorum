import boto3
import json
import time

def lambda_handler(event, context):
    client = boto3.client('redshift-data')
    
    # Retrieve database connection information from environment variables
    workgroup_name = 'default-workgroup'
    database = 'dev'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:339713000240:secret:prod-dw-access-H7nfCP'

    copy_sql = """
    COPY gdelt_event
    FROM 's3://gdelt-project/dependencies/manifest.json'
    IAM_ROLE 'arn:aws:iam::339713000240:role/RedshiftRole'
    FORMAT AS CSV
    DELIMITER '\t'
    IGNOREHEADER 0
    MANIFEST;
    """
    
    try:
        # Execute the COPY command using the specified secret for authentication
        response = client.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            SecretArn=secret_arn,
            Sql=copy_sql
        )
        
        execution_id = response['Id']
        print(f"Query execution ID: {execution_id}")
        
        while True:
            status_response = client.describe_statement(Id=execution_id)
            status = status_response['Status']
            print(f"Query execution status: {status}")
            
            if status == 'FINISHED':
                print("COPY command completed successfully.")
                break
            elif status == 'FAILED':
                print(f"Query failed: {status_response['Error']}")
                return {
                    'statusCode': 500,
                    'body': json.dumps(f"Query failed with error: {status_response['Error']}")
                }
            else:
                print("Waiting for query to complete...")
                time.sleep(5) 
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Query execution completed with status: {status}")
        }
    
    except Exception as e:
        print(f"Error executing the query: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing the query: {str(e)}")
        }
