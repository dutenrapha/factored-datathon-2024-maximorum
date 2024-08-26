# Maximorum GDELT Project

project site: https://theproject.algoritmosacademy.com/

## Overview
The Maximorum project integrates the GDELT event dataset with advanced machine learning techniques to optimize supply chain decisions, focusing on minimizing risks associated with social unrest in specific countries. Our product combines real-time global data with anomaly detection using Self-Organizing Maps (SOM) to assess and mitigate supply chain risks, allowing businesses to balance cost efficiency with the potential risks of disruptions.

## Key Features

### Maximorum Factored 2024 Datathon
This project was developed as part of the Maximorum Factored 2024 Datathon, where participants were challenged to build solutions for optimizing supply chain decisions in volatile international markets.

### Data Pipeline
- **AWS Lambda:** Automates data extraction from GDELT, comparing newly available data with an existing manifest and downloading only new files.
- **AWS Glue:** Handles data transformation, unzipping files, and preparing them for analysis.
- **AWS Redshift:** Acts as the Data Warehouse, storing validated and processed data for efficient querying and analytics.

### Risk Assessment and Machine Learning
- **Anomaly Detection with SOM:** Identifies countries with unusual patterns in events that may pose risks to supply chains.
- **ARIMA Models:** Used for time series forecasting, predicting potential risks for the next week based on historical data.

### Frontend Development
- **Streamlit Application:** Provides an interactive interface for users to input supply chain details, analyze risk levels, and optimize decisions based on both cost and risk factors.

## Technical Details

### Data Analysis
We analyzed six key variables from the GDELT dataset—totalmentions, totalsources, totalarticles, medianavgtone, mediangoldsteinscale, and EventRootCode. These variables were selected to construct a robust risk model. To address data gaps due to daily aggregation, we shifted to weekly data, which provided a more reliable basis for forecasting and anomaly detection.

### Machine Learning and Model Deployment
- **Feature Engineering:** Simple yet effective transformations were applied to ensure the integrity of the data while optimizing model performance.
- **Model Deployment:** The SOM model was deployed using AWS Lambda due to its lightweight nature, with the serialized model stored in S3 for easy access.

### Infrastructure and Deployment
- **Containerization with Docker and Pants:** Applications were containerized and deployed using AWS services, with Docker images managed through Amazon ECR.
- **Scalable Backend:** The backend uses parallelized Lambda functions for efficient processing, ensuring fast execution times and scalability.

## Installation

### Prerequisites
- Python 3.8 or later
- Docker
- AWS CLI
- Terraform
