'''
This module provides functions for data extraction from a PostgreSQL database using Spark.

Example jdbc_properties:
    jdbc_properties = {
        'user': 'your_username',
        'password': 'your_password',
        'driver': 'org.postgresql.Driver'
    }

    Example usage:
    spark = SparkSession.builder.appName('MyApp').getOrCreate()
    extracted_data = extract(
        spark,
        'jdbc:postgresql://localhost:5432/mydb',
        jdbc_properties,
        'my_table'
    )
    spark.stop()

'''
def extract(spark, jdbc_url, jdbc_properties, table_name):
    '''
    Extract data from a PostgreSQL database table using Spark.

    parameters:
        spark: The SparkSession.
        jdbc_url: The JDBC URL for the PostgreSQL database.
        jdbc_properties: A dictionary of JDBC connection properties.
        table_name: The name of the table in the database to extract data from.

    return: A DataFrame containing the extracted data.
    '''
    data_frame = spark.read.jdbc(
        jdbc_url,
        table_name,
        properties=jdbc_properties
    )

    return data_frame
