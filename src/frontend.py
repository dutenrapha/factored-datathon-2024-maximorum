import os
import json
import streamlit as st
import boto3


def load_data(file_name):
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, file_name)
    with open(file_path, 'r') as file:
        return json.load(file)


COUNTRY_MAP = load_data('countries.json')

def get_supply_chain_items():
    items = []
    
    num_items = st.number_input("How many items are in your supply chain?", min_value=1, step=1)
    
    for i in range(num_items):
        item_name = st.text_input(f"Item name {i + 1}")
        
        countries_costs = []
        num_countries = st.number_input(f"How many countries produce the item {item_name}?", min_value=1, step=1, key=f"num_countries_{i}")
        
        for j in range(num_countries):
            country_name = st.selectbox(f"Country {j + 1} for item {item_name}", options=list(COUNTRY_MAP.values()), key=f"country_{i}_{j}")
            country_code = list(COUNTRY_MAP.keys())[list(COUNTRY_MAP.values()).index(country_name)]
            cost = st.number_input(f"Production cost in {country_name}", min_value=0.0, step=0.01, key=f"cost_{i}_{j}")
            countries_costs.append({country_code: cost})
        
        items.append({item_name: countries_costs})
    
    return items

def display_example_data(example_data):
    for item in example_data['items']:
        item_name, countries_costs = list(item.items())[0]
        st.markdown(f"**Item:** {item_name}")
        for cost in countries_costs:
            country_code, price = list(cost.items())[0]
            st.write(f"- {COUNTRY_MAP.get(country_code, country_code)}: {price}")
    st.markdown(f"**Risk Aversion:** {example_data['risk_aversion']}")

def invoke_lambda(payload):
    lambda_client = boto3.client('lambda', region_name='us-east-2') 

    response = lambda_client.invoke(
        FunctionName='minimize',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )

    response_payload = json.loads(response['Payload'].read())
    
    if response_payload.get("statusCode") == 200:
        recommendations = json.loads(response_payload.get("body"))
        return recommendations
    else:
        raise Exception("Failed to get a valid response from the Lambda function.")

def main():
    st.title("Supply Chain Risk Management")

    example_filled = st.checkbox("Example Filled")

    if example_filled:
        example_data = load_data('example.json')
        st.markdown("### Example Data:")
        display_example_data(example_data)
        items = example_data['items']
        risk_aversion = example_data['risk_aversion']
    else:
        items = get_supply_chain_items()
        risk_aversion = st.selectbox("What is your risk aversion level?", ["High", "Medium", "Low"])

    if st.button("Minimize Risk"):
        st.write("Sending data to `minimize` Lambda function...")
        payload = {"items": items, "risk_aversion": risk_aversion}
        
        try:
            recommendations = invoke_lambda(payload)
            st.write("Production recommendations received:")
            for recommendation in recommendations:
                for item_name, best_country in recommendation.items():
                    st.write(f"Item: {item_name} should be produced in {COUNTRY_MAP.get(best_country, best_country)}")
        except Exception as e:
            st.write(f"Failed to invoke Lambda function. Error: {str(e)}")

if __name__ == "__main__":
    main()
