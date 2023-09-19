# Batch Processing with PySpark

This project demonstrates batch processing with PySpark using docker which contains the following containers.

## Table of Contents
- [Architecture](#architecture)
- [Dataset](#dataset)
- [How to run](#how-to-run)
- [Docker setup troubleshooting](#troubleshooting)
- [Spark Scripts](#sparkscripts)
- [Analysis result](#analysis-result)


## Architecture

## Dataset
The dataset used in this project is `online-retail-dataset` which is from UCI Machine Learning Repository.
The dataset is a real online retail transaction dataset about a UK-based non-store online retail which mainly sells unique all-occasion gift-ware.

### Dataset Information

- **InvoiceNo**: Invoice number. Nominal. A 6-digit integral number uniquely assigned to each transaction. If this code starts with the letter 'c', it indicates a cancellation.

- **StockCode**: Product (item) code. Nominal. A 5-digit integral number uniquely assigned to each distinct product.

- **Description**: Product (item) name. Nominal.

- **Quantity**: The quantities of each product (item) per transaction. Numeric.

- **InvoiceDate**: Invoice date and time. Numeric. The day and time when a transaction was generated.

- **UnitPrice**: Unit price. Numeric. Product price per unit in sterling.

- **CustomerID**: Customer number. Nominal. A 5-digit integral number uniquely assigned to each customer.

- **Country**: Country name. Nominal. The name of the country where a customer resides.


## How to run
1. Clone this repository
2. Open terminal / CMD and change the directory to the cloned repository folder
3. For x86 user, run:
    ```console
    make docker-build
    ```
    For arm chip user, edit "docker-compose-airflow.yml" file in "docker" folder by changing `amd64` to `arm64` in all platform settings, then run:
    ```console
    make docker-build-arm
    ```
    ![Make Docker Build](/img/make-docker-build.png)
4. After the setup is complete, run these three commands in order: 

    `make postgres`
    ![Make postgres](/img/make-postgres.png)
    
    `make spark`

    ![Make Spark](/img/make-spark.png)
    `make airflow`
    ![Make Airflow](/img/make-airflow.png)
 
5. Wait until the airflow setup is already, then access airflow Webserver UI in your web browser access: `localhost:8081`
    ![airflow home](/img/airflow-home.png)
    then run the dag by manually trigger it with clicking the small play button at the top right of the UI.
    ![airflow dag](/img/aiflow-dag.png)

6. Since the result of the task is loading a dataset / table into postgreSQL, you can check the table by connecting postgreSQL database we made by using Dbeaver. You can check the postgres setup at `.env `file. 
    ![make postgres1](/img/connect-postgres-1.png)
    ![make postgres2](/img/connect-postgres-2.png)
    ![make postgres3](/img/connect-postgres-3.png)


## Docker Setup Troubleshooting
### `make airflow` error because of `entrypoint.sh`
This error happens because `entrypoint.sh` cannot be detected at the specified path. 
    ![entrypoint1](/img/entrypoint-error-in-CRLF.png)
There are many possible solutions, one of the solution is to open the `entrypoint.sh` at VSCode then change the encoding to UTF-8 if it is not and try to change the end line sequence from CRLF to LF and save the changes. 
    ![entrypoint2](/img/entrypoint-error-in-CRLF-2.png)
    ![entrypoint3](/img/entrypoint-error-in-CRLF-3.png)
Then either run `make airflow` again or restart the setup from `make docker-build` again should solve the problem.
## Spark Scripts
The structure of the Spark Scripts folder is like this.
- spark-scripts
  - main.py
  - lib
    - extract.py
    - transform.py
    - load.py
    - __init__.py
  - jar
    - postgresql-42.6.0.jar

The jar file is to help spark connect to postgreSQL.


## ETL Result
### Adding `is_cancelled` Column
Here, the retail dataset is enriched by adding `is_cancelled` column to make it easier to further analyze the cancelled transaction for future projects whether for machine learning, like customer segmentation or maybe sentiment analysis with the help of analyzing `is_cancelled` column and `description` column. Here in this project, I used this column to find completion rate.

### Data Cleaning
For the data cleaning, after checking the datasets, there are some interesting things:

![Cleaned Retail](/img/postgres-retail-cleaned.png)


### Completion Rate
![Cleaned Retail](/img/postgres-completion-rate.png)
### Monthly Churn Rate
![Cleaned Retail](/img/postgres-churn-rate.png)

