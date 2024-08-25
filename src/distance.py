import boto3
import json
import pickle
import time
import numpy as np
from minisom import MiniSom
from concurrent.futures import ThreadPoolExecutor, as_completed

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    client = boto3.client('redshift-data')
    
    s3_bucket = 'gdelt-project'
    s3_key = 'dependencies/minisom_model.pkl'
    
    workgroup_name = 'default-workgroup'
    database = 'dev'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:339713000240:secret:prod-dw-access-H7nfCP'
    forecast_table = 'forecast'
    distance_table = 'distance'

    try:
        # Carregar o modelo MiniSom do S3
        with open('/tmp/minisom_model.pkl', 'wb') as f:
            s3_client.download_fileobj(s3_bucket, s3_key, f)

        with open('/tmp/minisom_model.pkl', 'rb') as f:
            som = pickle.load(f)

        # Deletar todos os dados da tabela distance antes de inserir novos dados
        delete_query = f"DELETE FROM {distance_table}"
        delete_response = client.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            SecretArn=secret_arn,
            Sql=delete_query
        )
        
        delete_execution_id = delete_response['Id']
        print(f"Delete execution ID: {delete_execution_id}")
        
        # Aguardando a conclusão da operação de delete
        while True:
            delete_status_response = client.describe_statement(Id=delete_execution_id)
            delete_status = delete_status_response['Status']
            print(f"Delete execution status: {delete_status}")
            
            if delete_status == 'FINISHED':
                print("Delete operation completed successfully.")
                break
            elif delete_status == 'FAILED':
                print(f"Delete operation failed: {delete_status_response['Error']}")
                return {
                    'statusCode': 500,
                    'body': json.dumps(f"Delete operation failed with error: {delete_status_response['Error']}")
                }
            else:
                print("Waiting for delete operation to complete...")
                time.sleep(5)

        # Recuperar os dados previstos da tabela forecast
        query = f"""
        SELECT country, TotalMentions, TotalSources, TotalArticles, MedianAvgTone, MedianGoldsteinScale
        FROM {forecast_table}
        """
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
                break;
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

        # Organizar os dados para cálculo das distâncias
        country_data = {}
        for record in records:
            country_code = record[0]['stringValue']
            data = [
                float(record[1]['stringValue']),
                float(record[2]['stringValue']),
                float(record[3]['stringValue']),
                float(record[4]['stringValue']),
                float(record[5]['stringValue'])
            ]
            if country_code not in country_data:
                country_data[country_code] = {'data': []}
            country_data[country_code]['data'].append(data)

        # Função para processar cada país
        def process_country(country_code, values):
            try:
                X_test = np.array(values['data'])
                distances = np.array([som.distance_map()[som.winner(x)] for x in X_test])

                for distance in distances:
                    insert_query = f"""
                    INSERT INTO {distance_table} (country, distance)
                    VALUES ('{country_code}', {distance})
                    """
                    insert_response = client.execute_statement(
                        WorkgroupName=workgroup_name,
                        Database=database,
                        SecretArn=secret_arn,
                        Sql=insert_query
                    )
                    
                    insert_execution_id = insert_response['Id']
                    print(f"Insert execution ID for country {country_code}: {insert_execution_id}")
                    
                    while True:
                        insert_status_response = client.describe_statement(Id=insert_execution_id)
                        insert_status = insert_status_response['Status']
                        print(f"Insert execution status for country {country_code}: {insert_status}")
                        
                        if insert_status == 'FINISHED':
                            print(f"Insert completed successfully for country {country_code}.")
                            break
                        elif insert_status == 'FAILED':
                            print(f"Insert failed for country {country_code}: {insert_status_response['Error']}")
                            return f"Failed for country {country_code}"
                        else:
                            print(f"Waiting for insert to complete for country {country_code}...")
                            time.sleep(5)
                return f"Success for country {country_code}"
            except Exception as e:
                print(f"Error processing country {country_code}: {str(e)}")
                return f"Failed for country {country_code}"

        # Executando as chamadas em paralelo usando ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=50) as executor:  # Ajuste o número de workers conforme necessário
            futures = {executor.submit(process_country, country_code, values): country_code for country_code, values in country_data.items()}

            for future in as_completed(futures):
                country_code = futures[future]
                try:
                    result = future.result()
                    print(f"Result for {country_code}: {result}")
                except Exception as e:
                    print(f"Exception for {country_code}: {str(e)}")

        return {
            'statusCode': 200,
            'body': json.dumps("Distance table updated successfully.")
        }

    except Exception as e:
        print(f"Error loading the model or updating the distance table: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error loading the model or updating the distance table: {str(e)}")
        }
