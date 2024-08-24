import boto3
import json
import time
from statsmodels.tsa.arima.model import ARIMA

def lambda_handler(event, context):
    client = boto3.client('redshift-data')
    
    if 'ActionGeo_CountryCode' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps("Error: 'ActionGeo_CountryCode' parameter is required.")
        }
    
    action_geo_country_code = event['ActionGeo_CountryCode']
    
    workgroup_name = 'default-workgroup'
    database = 'dev'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:339713000240:secret:prod-dw-access-H7nfCP'

    query = f"""
    WITH recent_weeks AS (
        SELECT DISTINCT DATE_TRUNC('week', TO_DATE(SQLDATE, 'YYYYMMDD')) AS Week
        FROM gdelt_event
        WHERE TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), -2)
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
                AND TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), -2)
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
                    AND TO_DATE(SQLDATE, 'YYYYMMDD') >= ADD_MONTHS(DATE_TRUNC('week', CURRENT_DATE), -2)
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

        data['Week'] = [time.strptime(week, '%Y-%m-%d') for week in data['Week']]

        predictions = {}

        for column in ['TotalMentions', 'TotalSources', 'TotalArticles', 'MedianAvgTone', 'MedianGoldsteinScale']:
            y = data[column]

            y_train = y[:-1]
            y_test = y[-1]

            best_model, best_order, best_score = find_best_arima_model(y_train, y_test)

            forecast = best_model.forecast(steps=1)
            predictions[column] = forecast[0]

        return {
            'statusCode': 200,
            'body': json.dumps(predictions)
        }
    
    except Exception as e:
        print(f"Error executing the query: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing the query: {str(e)}")
        }

def find_best_arima_model(y_train, y_test):
    best_score = float('inf')
    best_order = None
    best_model = None
    
    for p in range(3):
        for d in range(3):
            for q in range(3):
                try:
                    model = ARIMA(y_train, order=(p, d, q))
                    model_fit = model.fit()
                    forecast = model_fit.forecast(steps=1)
                    
                    mse = mean_squared_error(y_test, forecast[0])
                    
                    if mse < best_score:
                        best_score = mse
                        best_order = (p, d, q)
                        best_model = model_fit
                except Exception as e:
                    continue

    return best_model, best_order, best_score

def mean_squared_error(actual, predicted):
    return (actual - predicted) ** 2
