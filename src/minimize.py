import json
import boto3
import cvxpy as cp
import numpy as np
import time

def get_risk_aversion_level(risk_aversion_level):
    print(f"Getting risk aversion level for: {risk_aversion_level}")
    risk_aversion_map = {
        "Low": 0.75,
        "Medium": 0.5,
        "High": 0.25
    }
    RA = risk_aversion_map.get(risk_aversion_level, 0.5)
    print(f"Risk aversion level (RA) set to: {RA}")
    return RA

def parse_items(event):
    print("Parsing items from event...")
    countries = []
    cost_dict = {}
    item_names = []

    for item in event["items"]:
        for item_name, country_cost_list in item.items():
            item_names.append(item_name)
            cost_for_item = {}
            for country_cost in country_cost_list:
                for country, cost in country_cost.items():
                    if country not in countries:
                        countries.append(country)
                    cost_for_item[country] = cost
            cost_dict[item_name] = cost_for_item
    
    print(f"Extracted countries: {countries}")
    print(f"Cost dictionary: {cost_dict}")
    print(f"Item names: {item_names}")

    n_items = len(item_names)
    n_countries = len(countries)
    
    cost = np.zeros((n_items, n_countries))  # Initialize cost matrix with zeros
    
    for i, item_name in enumerate(item_names):
        for j, country in enumerate(countries):
            cost[i, j] = cost_dict[item_name].get(country, 0)  # Set cost, defaulting to 0
    
    print(f"Cost matrix created with shape: {cost.shape}")
    print(f"Cost matrix: \n{cost}")
    assert cost.shape == (n_items, n_countries), f"Expected cost shape {(n_items, n_countries)}, got {cost.shape}"

    return countries, cost, item_names

def query_distance_table(client, workgroup_name, database, secret_arn, distance_table):
    print(f"Querying distance table: {distance_table}")
    query = f"SELECT country, distance FROM {distance_table}"
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
            raise Exception(f"Query failed: {status_response['Error']}")
        time.sleep(5)

    result_response = client.get_statement_result(Id=execution_id)
    print(f"Query result received with {len(result_response['Records'])} records.")
    return result_response['Records']

def build_distance_map(records):
    print("Building distance map from records...")
    distance_map = {}
    for record in records:
        country_code = record[0]['stringValue']
        distance = float(record[1]['stringValue'])
        distance_map[country_code] = distance
    print(f"Distance map: {distance_map}")
    return distance_map

def calculate_distances(countries, distance_map):
    print(f"Calculating distances for countries: {countries}")
    distance = np.array([distance_map.get(country, float('inf')) for country in countries])
    print(f"Distance vector: {distance}")
    return distance

def validate_distances(distance, countries):
    print("Validating distances...")
    if np.any(np.isinf(distance)):
        missing_countries = [countries[i] for i in range(len(countries)) if np.isinf(distance[i])]
        print(f"Missing distances for countries: {missing_countries}")
        raise ValueError(f"Missing distances for countries: {missing_countries}")
    print("All distances are valid.")

def solve_optimization_problem(cost, distance, RA, n_items, n_countries):
    print(f"Solving optimization problem with the following parameters:")
    print(f"Cost matrix: \n{cost}")
    print(f"Distance vector: \n{distance}")
    print(f"Risk Aversion (RA): {RA}")
    
    xi = cp.Variable((n_items, n_countries), boolean=True)
    
    assert cost.shape == xi.shape, f"Shape mismatch: cost {cost.shape}, xi {xi.shape}"
    
    objective = cp.Minimize(cp.sum(cp.multiply(cost, xi)))
    constraints = [cp.sum(xi[i, :]) == 1 for i in range(n_items)]
    constraints.append(cp.sum(cp.multiply(distance, cp.sum(xi, axis=0))) <= RA)
    
    problem = cp.Problem(objective, constraints)
    problem.solve()
    
    print(f"Optimization problem solved. Status: {problem.status}")
    return xi

def format_result(xi, item_names, countries, n_items, n_countries):
    print("Formatting optimization results...")
    result = []
    for i in range(n_items):
        for c in range(n_countries):
            if xi.value[i, c] > 0.5:
                result.append({item_names[i]: countries[c]})
    print(f"Formatted result: {result}")
    return result

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    client = boto3.client('redshift-data')
    
    workgroup_name = 'default-workgroup'
    database = 'dev'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:339713000240:secret:prod-dw-access-H7nfCP'
    distance_table = 'distance'
    
    try:
        print("Lambda handler started.")
        RA = get_risk_aversion_level(event["risk_aversion"])
        countries, cost, item_names = parse_items(event)
        n_countries = len(countries)
        n_items = len(item_names)
        
        records = query_distance_table(client, workgroup_name, database, secret_arn, distance_table)
        distance_map = build_distance_map(records)
        distance = calculate_distances(countries, distance_map)
        
        validate_distances(distance, countries)
        
        xi = solve_optimization_problem(cost, distance, RA, n_items, n_countries)
        result = format_result(xi, item_names, countries, n_items, n_countries)
        
        print("Lambda handler completed successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
