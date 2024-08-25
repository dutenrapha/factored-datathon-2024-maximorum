import boto3
import json
import time
import pickle
import numpy as np
from minisom import MiniSom

def lambda_handler(event, context):
    client = boto3.client('redshift-data')
    s3_client = boto3.client('s3')
    
    workgroup_name = 'default-workgroup'
    database = 'dev'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:339713000240:secret:prod-dw-access-H7nfCP'
    s3_bucket = 'gdelt-project'
    s3_key = 'dependencies/minisom_model.pkl'

    # Recebendo o número de meses como parâmetro do evento
    num_months = event.get('num_months', -4)  # Valor padrão de -4 se não for especificado

    query = f"""
    WITH recent_weeks AS (
        SELECT DISTINCT DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD')) AS Week
        FROM gdelt_event
        WHERE TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), {num_months})
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
                AND TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), {num_months})
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
                    AND TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), {num_months})
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
        response = client.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        execution_id = response['Id']
        print(f"Query execution ID: {execution_id}")
        
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

        result_response = client.get_statement_result(Id=execution_id)
        records = result_response['Records']

        data = {
            'Week': [],
            'TotalMentions': [],
            'TotalSources': [],
            'TotalArticles': [],
            'MedianAvgTone': [],
            'MedianGoldsteinScale': []
        }

        for record in records:
            week_str = record[0]['stringValue'].split(" ")[0]
            data['Week'].append(week_str)
            data['TotalMentions'].append(float(record[1]['longValue']))
            data['TotalSources'].append(float(record[2]['longValue']))
            data['TotalArticles'].append(float(record[3]['longValue']))
            data['MedianAvgTone'].append(float(record[4]['stringValue']))
            data['MedianGoldsteinScale'].append(float(record[5]['stringValue']))

        # Prepare data for MiniSom and convert to NumPy array
        X_train = np.array([
            [
                data['TotalMentions'][i],
                data['TotalSources'][i],
                data['TotalArticles'][i],
                data['MedianAvgTone'][i],
                data['MedianGoldsteinScale'][i]
            ]
            for i in range(len(data['Week']))
        ])

        # Initialize and train the MiniSom model
        print(f"X_Train: {X_train}")
        som = MiniSom(5, 5, X_train.shape[1], sigma=1.0, learning_rate=0.5)
        som.random_weights_init(X_train)
        som.train_random(X_train, 100)

        # Serialize the MiniSom model
        with open('/tmp/minisom_model.pkl', 'wb') as f:
            pickle.dump(som, f)

        # Upload the model to S3
        s3_client.upload_file('/tmp/minisom_model.pkl', s3_bucket, s3_key)

        return {
            'statusCode': 200,
            'body': json.dumps(f"MiniSom model successfully saved to s3://{s3_bucket}/{s3_key}")
        }
    
    except Exception as e:
        print(f"Error executing the query or training the model: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing the query or training the model: {str(e)}")
        }
