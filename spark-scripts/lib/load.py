'''
This module provides a function for loading a DataFrame into PostgreSQL database table.

Usage:
    from my_module import load

    # Example JDBC properties
    jdbc_properties = {
        'user': 'your_username',
        'password': 'your_password',
        'driver': 'org.postgresql.Driver',
    }

    # Example usage
    result = load(
        my_data_frame,
        'jdbc:postgresql://localhost:5432/mydb',
        jdbc_properties,
        'my_table'
    )
    print(result)
'''
def load(data_frame, jdbc_url, jdbc_properties, load_name):
    '''
    Load a DataFrame into a PostgreSQL database table.

    Parameters:
        data_frame: The DataFrame to be loaded.
        jdbc_url: The JDBC URL for the PostgreSQL database.
        jdbc_properties: A dictionary of JDBC connection properties.
        load_name: The name of the table in PostgreSQL where the DataFrame will be loaded.

    Returns:
        str: A success message indicating the table load status.
    '''
    data_frame.write.mode('overwrite').jdbc(
        url=jdbc_url,
        table=load_name,
        mode='overwrite',
        properties=jdbc_properties)

    return f'Successfully loaded {load_name} to the desired PostgreSQL database'
