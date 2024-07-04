# Welcome to SuburbSavvy

SuburbSavvy is a simple first project exploring how to use PySpark, PostgreSQL, and MySQL to load a sample dataset into PostgreSQL, perform basic data cleaning and preparation tasks, and then writing to a MySQL database.

## About

SuburbSavvy is a basic data pipeline designed to:

1. Ingest data
2. Clean and prepare data
3. Process and analyze data
4. Export data

## Features

1. Reading and writing: Reading and writing to multiple sources.
2. Error Handling: Includes basic error handling for PySpark SQL operations.
3. Context Manager: Utilizes a context manager to manage the Spark session lifecycle.
4. Spark Session: The Spark session is configured to run locally, utilizing all available logical processors for parallelizing the workload.

## Todos

- Task Automation: Automate via a task runner like Airflow to trigger execution based on events or schedules.
- Spark Optimization: Refine the configuration to fully explore how Spark can be optimized for processing large datasets, e.g., explore - parallelizing the workload.
- Cloud Integration for a scalable environment: Utilize cloud resources such as Azure HDInsight to explore customizing things like the number of cluster nodes.
- Error Handling: Improved error handling with the addition of PySpark SQL error handling.

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/staceyjf/SuburbSavvy
   cd SuburbSavvy
   ```

2. **Set up the virtual environment using pipenv:**

   ```bash
   pip install pipenv
   pipenv install
   pipenv shell
   ```

3. **Set up a PostgreSQL and a mySql database:**
   - Make sure you have PostgreSQL and mySql installed and running.
   - Create databases for the project.
   - Create an env file. An example env file can be found in `env_template.txt`

## Usage

1. **Load the sample data into PostgreSQL, perform data cleaning and preparation and write to a mySql database:**
   ```bash
   python src/app.py
   ```
