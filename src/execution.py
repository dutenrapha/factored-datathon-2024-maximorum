import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def lambda_handler(event, context):
    client = boto3.client('redshift-data')
    lambda_client = boto3.client('lambda')
    workgroup_name = 'default-workgroup'
    database = 'dev'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:339713000240:secret:prod-dw-access-H7nfCP'
    table_name = 'forecast'

    query = """
    SELECT DISTINCT(ActionGeo_CountryCode)
    FROM gdelt_event
    """

    try:
        # Executando a consulta no Redshift
        response = client.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            SecretArn=secret_arn,
            Sql=query
        )
        
        execution_id = response['Id']
        print(f"Query execution ID: {execution_id}")
        
        # Aguardando a conclusão da consulta
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

        # Deletando todos os dados da tabela forecast antes de inserir novos dados
        delete_query = f"DELETE FROM {table_name}"
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

        # Recuperando os resultados da consulta
        result_response = client.get_statement_result(Id=execution_id)
        records = result_response['Records']

        # Função para processar cada país
        def process_country(country_code):
            try:
                # Chamando a segunda Lambda Function para obter as métricas
                forecast_response = lambda_client.invoke(
                    FunctionName='arn:aws:lambda:us-east-2:339713000240:function:forecast_metrics',
                    InvocationType='RequestResponse',
                    Payload=json.dumps({"ActionGeo_CountryCode": country_code})
                )
                
                forecast_result = json.loads(forecast_response['Payload'].read())
                forecast_data = json.loads(forecast_result['body'])

                # Verificação para garantir que a resposta seja válida
                if 'TotalMentions' not in forecast_data or 'TotalSources' not in forecast_data or 'TotalArticles' not in forecast_data or 'MedianAvgTone' not in forecast_data or 'MedianGoldsteinScale' not in forecast_data:
                    raise ValueError("Invalid response structure")

            except Exception as e:
                print(f"Error processing country {country_code}: {str(e)}. Filling with zeros.")
                # Se houver erro, definimos os valores como zero
                forecast_data = {
                    'TotalMentions': 0,
                    'TotalSources': 0,
                    'TotalArticles': 0,
                    'MedianAvgTone': 0,
                    'MedianGoldsteinScale': 0
                }

            # Atualizando a tabela forecast no Redshift
            try:
                update_query = f"""
                INSERT INTO {table_name} (country, TotalMentions, TotalSources, TotalArticles, MedianAvgTone, MedianGoldsteinScale)
                VALUES ('{country_code}', {forecast_data['TotalMentions']}, {forecast_data['TotalSources']}, {forecast_data['TotalArticles']}, {forecast_data['MedianAvgTone']}, {forecast_data['MedianGoldsteinScale']})
                """
                
                update_response = client.execute_statement(
                    WorkgroupName=workgroup_name,
                    Database=database,
                    SecretArn=secret_arn,
                    Sql=update_query
                )
                
                update_execution_id = update_response['Id']
                print(f"Update execution ID for country {country_code}: {update_execution_id}")

                # Aguardando a conclusão da atualização
                while True:
                    update_status_response = client.describe_statement(Id=update_execution_id)
                    update_status = update_status_response['Status']
                    print(f"Update execution status for country {country_code}: {update_status}")
                    
                    if update_status == 'FINISHED':
                        print(f"Update for country {country_code} completed successfully.")
                        break
                    elif update_status == 'FAILED':
                        print(f"Update failed for country {country_code}: {update_status_response['Error']}")
                        return {
                            'statusCode': 500,
                            'body': json.dumps(f"Update failed for country {country_code} with error: {update_status_response['Error']}")
                        }
                    else:
                        print(f"Waiting for update to complete for country {country_code}...")
                        time.sleep(5)

                return f"Success for country {country_code}"

            except Exception as e:
                print(f"Error updating forecast table for country {country_code}: {str(e)}")
                return f"Failed for country {country_code}"

        # Executando as chamadas em paralelo usando ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=50) as executor:  # Ajuste o número de workers conforme necessário
            futures = {executor.submit(process_country, record[0]['stringValue']): record[0]['stringValue'] for record in records}

            for future in as_completed(futures):
                country_code = futures[future]
                try:
                    result = future.result()
                    print(f"Result for {country_code}: {result}")
                except Exception as e:
                    print(f"Exception for {country_code}: {str(e)}")

        return {
            'statusCode': 200,
            'body': json.dumps("Forecast table updated successfully.")
        }
    
    except Exception as e:
        print(f"Error executing the query or updating the forecast table: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing the query or updating the forecast table: {str(e)}")
        }
