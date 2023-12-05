# Fraud Detection in Financial Transactions

## Project Overview

The **Fraud Detection in Financial Transactions** project is aimed at addressing the increasing challenge of fraudulent transactions for "FinTech Innovations." The project implements a robust system that analyzes transactional data, customer information, and external data to detect suspicious activities in near real-time, with a focus on minimizing false alerts.

## Project Objectives

The key objectives of the project include:

1. **API Development:** Creation of APIs for transaction data, customer data, and external data to facilitate seamless access.

2. **Data Collection and Integration:** Utilizing APIs to collect transactional, customer, and external data and ensuring data cleanliness and relevance.

3. **Storage and Data Management with Hive:** Designing and implementing Hive tables for efficient storage and management of transaction, customer, and external data.

4. **Rule-Based Fraud Detection System:** Development of HiveQL queries for rule-based fraud detection, including the identification of unusually high transactions, high-frequency transactions, and transactions involving blacklisted customers.

5. **Deployment:** Implementation of an Airflow DAG for orchestration of data collection, processing, and alerting, along with CI/CD integration using GitHub Actions.

## Implementation Details

### Data Generation (`data_generator.py`)

This script generates synthetic data for transactions, customers, and external data, implementing APIs for data access and saving the generated data in JSON files.

### API Server (`api.py`)

Flask APIs for transactions, customers, and external data are implemented in this script. It loads data from JSON files for API responses.

### Data Loading (`load.py`)

This script fetches data from APIs using Python requests, connects to Hive, creates tables for transactions, customers, blacklist, and external info, and loads data into Hive tables.

### Fraud Detection (`fraud_detection_HiveQL.ipynb`)

Connects to Hive for fraud detection queries using HiveQL, executing queries to identify transactions with unusually high amounts, transactions involving blacklisted customers, and transactions from unusual locations.

### DAG Orchestration (`tasks.py`)

Airflow DAG that orchestrates the entire workflow:

1. **Task 1 - Data Generation:** Generates synthetic data using `data_generator.py`.
2. **Task 2 - API:** Launches the Flask API server using `api.py`.
3. **Task 3 - Load Data:** Executes a Jupyter Notebook (`load.ipynb`) to fetch and load data into Hive tables.
4. **Task 4 - Fraud Detection:** Executes a Jupyter Notebook (`fraud_detection_HiveQL.ipynb`) for fraud detection using HiveQL queries.


### Prerequisites

- Python
- Flask
- Apache Hive
- Apache Airflow
