'''
This script extracts, transforms, and loads retail data using PySpark into a PostgreSQL database.

It performs the following steps:
1. Reads configurations from a .env file.
2. Sets up a PySpark context and Spark session.
3. Connects to a PostgreSQL database.
4. Extracts retail data from PostgreSQL.
5. Cleans and transforms the data.
6. Computes completion rates and ranks by country.
7. Calculates monthly customer churn rates.
8. Loads the cleaned and transformed data into PostgreSQL.

Usage:
    This script is to be executed by Spark Submit operator which is called by airflow
'''
# Import libraries to be used
import os
from pathlib import Path
import pyspark
from dotenv import load_dotenv
from lib.extract import extract
from lib.transform import *
from lib.load import load


def main():
    '''
    Main function to execute the ETL process for retail data.
    '''
    # Read .env file
    dotenv_path = Path('../.env')
    load_dotenv(dotenv_path=dotenv_path)


    # Setup PySpark
    # Initialize spark context in Spark Master
    sparkcontext = pyspark.SparkContext.getOrCreate(
        conf=(
            pyspark
            .SparkConf()
            .setAppName('Dibimbing')
            .setMaster('spark://dataeng-spark-master:7077')
            .set('spark.jars', '/spark-scripts/postgresql-42.6.0.jar')
        )
    )
    sparkcontext.setLogLevel('WARN')

    # Define Spark Session
    spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())


    # Connect to postresql
    # variables for jdbc url and properties
    postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
    postgres_db = os.getenv('POSTGRES_DB')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')

    # Defining jdbc url and properties
    jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_db}'
    jdbc_properties = {
        'user': postgres_user,
        'password': postgres_password,
        'driver': 'org.postgresql.Driver',
        'stringtype': 'unspecified'
    }


    # Extract retail data frame from postfres
    table_name = 'public.retail'
    df_retail = extract(spark, jdbc_url, jdbc_properties, table_name)


    # Data cleaning and transformation
    # Add is_cancelled column to data frame
    df_retail = check_cancelled(df_retail)
    df_retail.show(5)

    # Clean retail data frame
    df_retail_cleaned = clean_retail(df_retail)
    # Load cleaned retail dataset to PostgreSQL
    load(df_retail_cleaned, jdbc_url, jdbc_properties, 'retail_cleaned')

    # Rank of Country based on completion rate
    df_completion_rank = completion_rank(df_retail_cleaned)
    df_completion_rank.show(5)
    # Load completion rate dataset to PostgreSQL
    load(df_completion_rank, jdbc_url, jdbc_properties, 'completion_rate_by_country')


    # Monthly customer churn rate
    # Filter out cancelled transactions
    df_not_cancelled = df_retail_cleaned.filter(df_retail_cleaned['is_cancelled'] == 'no')
    df_monthly_churn = monthly_churn(df_not_cancelled)
    # Load monthly customer churn rate dataset to PostgreSQL
    load(df_monthly_churn, jdbc_url, jdbc_properties, 'monthly_churn_rate')

    # stop spark session
    spark.stop()

if __name__ == '__main__':
    main()
