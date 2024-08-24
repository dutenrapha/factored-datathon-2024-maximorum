import boto3
import json
import os
import time
import pandas as pd

def lambda_handler(event, context):
    client = boto3.client('redshift-data')
    
    # Retrieve the mandatory ActionGeo_CountryCode parameter
    if 'ActionGeo_CountryCode' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps("Error: 'ActionGeo_CountryCode' parameter is required.")
        }
    
    action_geo_country_code = event['ActionGeo_CountryCode']
    
    # Define workgroup, database, and secret ARN
    workgroup_name = 'default-workgroup'
    database = 'dev'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:339713000240:secret:prod-dw-access-H7nfCP'

    query = f"""
    WITH recent_weeks AS (
        SELECT DISTINCT DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD')) AS Week
        FROM gdelt_event
        WHERE TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), -3)
    )
    SELECT 
        aggregated_data.Week,
        aggregated_data.TotalMentions,
        aggregated_data.TotalSources,
        aggregated_data.TotalArticles,
        median_data.AvgTone AS MedianAvgTone,
        median_data.GoldsteinScale AS MedianGoldsteinScale
    FROM 
        (
            SELECT
                DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD')) AS Week,
                SUM(NumMentions) AS TotalMentions,
                SUM(NumSources) AS TotalSources,
                SUM(NumArticles) AS TotalArticles
            FROM 
                gdelt_event
            WHERE 
                EventRootCode IN ('6', '7', '13', '14', '15', '16', '17', '18', '19', '20')
                AND ActionGeo_CountryCode = '{action_geo_country_code}'
                AND TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), -3)
            GROUP BY 
                DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD'))
        ) AS aggregated_data
    JOIN 
        (
            SELECT 
                Week,
                AVG(AvgTone) AS AvgTone,
                AVG(GoldsteinScale) AS GoldsteinScale
            FROM (
                SELECT 
                    DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD')) AS Week,
                    AvgTone,
                    GoldsteinScale,
                    NTILE(2) OVER (PARTITION BY DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD')) ORDER BY AvgTone) AS tone_ntile,
                    NTILE(2) OVER (PARTITION BY DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD')) ORDER BY GoldsteinScale) AS goldstein_ntile
                FROM 
                    gdelt_event
                WHERE 
                    EventRootCode IN ('6', '7', '13', '14', '15', '16', '17', '18', '19', '20')
                    AND ActionGeo_CountryCode = '{action_geo_country_code}'
                    AND TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), -3)
            ) AS subquery
            WHERE tone_ntile = 1 AND goldstein_ntile = 1
            GROUP BY 
                Week
        ) AS median_data
    ON 
        aggregated_data.Week = median_data.Week
    WHERE aggregated_data.Week IN (SELECT Week FROM recent_weeks)
    ORDER BY 
        aggregated_data.Week DESC;
    """

    try:
        # Execute the SQL query
        response = client.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        execution_id = response['Id']
        print(f"Query execution ID: {execution_id}")
        
        # Wait for the query to complete
        while True:
            status_response = client.describe_statement(Id=execution_id)
            status = status_response['Status']
            print(f"Query execution status: {status}")
            
            if status == 'FINISHED':
                print("Query completed successfully.")
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

        # Fetch the results
        result_response = client.get_statement_result(Id=execution_id)
        records = result_response['Records']
        
        # Convert records to a DataFrame
        data = []
        for record in records:
            row = []
            for field in record:
                if 'stringValue' in field:
                    row.append(field['stringValue'])
                elif 'doubleValue' in field:
                    row.append(field['doubleValue'])
                elif 'longValue' in field:
                    row.append(field['longValue'])
                else:
                    row.append(None)
            data.append(row)
        
        df = pd.DataFrame(data, columns=[
            'Week', 
            'TotalMentions', 
            'TotalSources', 
            'TotalArticles', 
            'MedianAvgTone', 
            'MedianGoldsteinScale'
        ])
        
        # Extract the first and last rows
        first_row = df.iloc[0].to_dict()
        last_row = df.iloc[-1].to_dict()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'first_row': first_row,
                'last_row': last_row,
                'len_df':len(df)
            })
        }
    
    except Exception as e:
        print(f"Error executing the query: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing the query: {str(e)}")
        }
