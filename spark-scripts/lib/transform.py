'''
This module contains functions to clean, transform and analyze retail data using PySpark.

Functions:
- check_cancelled: Add an 'is_cancelled' column based on 'invoiceno'.
- clean_retail: Clean the retail DataFrame by removing duplicates and filtering NULL values.
- completion_rank: Calculate completion rates and ranks by country.
- monthly_churn: Calculate monthly churn rates for customers.
'''
# Import libraries to be used
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def check_cancelled(df_retail):
    '''
    Add an 'is_cancelled' column to the DataFrame based on the 'invoiceno' column.

    parameters:
        df_retail: The DataFrame containing retail data.

    return:
        The DataFrame with the 'is_cancelled' column added.
    '''
    df_with_cancelled = df_retail.withColumn(
        'is_cancelled',
        F.when(F.col('invoiceno').startswith('C'), 'yes').otherwise('no')
    )

    return df_with_cancelled


def clean_retail(df_retail):
    '''
    Clean the retail DataFrame by removing duplicates, NULL values, and 0 unit price.

    parameters:
        df_retail: The DataFrame containing retail data.

    return:
        The cleaned DataFrame.
    '''
    # Drop rows with duplicates
    df_cleaned = df_retail.dropDuplicates()
    # Drop nulls from customerid and description
    # Drop quantity and unitprice with 0 value
    df_cleaned = df_cleaned.filter(
        F.col('customerid').isNotNull() &
        F.col('description').isNotNull() &
        (F.col('quantity') != 0) &
        (F.col('unitprice') != 0)
    )

    return df_cleaned


def completion_rank(data_frame):
    '''
    Calculate completion rates and ranks by country.

    parameters:
        data_frame: The DataFrame containing retail data.

    return:
        A DataFrame with completion rates and ranks.
    '''
    # Find completed transaction of each country
    country_completed_transaction = data_frame.filter(
        data_frame['is_cancelled'] == 'no') \
        .groupBy('country') \
        .agg(F.countDistinct('invoiceno').alias('completed_invoiceno'))

    # Find total transactions of each country
    # Including cancelled transactions
    country_total_transaction = data_frame.groupBy(
        'country').agg(F.countDistinct('invoiceno').alias('total_invoiceno')
    )

    # Find completion rate of each country
    df_completion_rate = country_completed_transaction.join(
        country_total_transaction,
        on='country',
        how='inner'
    ).withColumn(
        'completion_rate',
        (F.col('completed_invoiceno') /
         F.col('total_invoiceno')).cast('decimal(10, 2)')
    )

    # Find rank of each country based on completion rate
    window_spec = Window.orderBy(F.desc('completion_rate'))
    df_rank = df_completion_rate.withColumn('rank', F.rank().over(window_spec))

    # Create a completion rate ranked DataFrame
    completion_rate_ranked = df_rank.select('country', 'completion_rate', 'rank')

    return completion_rate_ranked


def monthly_churn(df_retail):
    '''
    Calculate monthly churn rates for customers.

    parameters:
        df_retail: The DataFrame containing retail data.

    return:
        A DataFrame with monthly churn rates.
    '''
    # Filter only transaction in 2011
    df_retail_2011 = df_retail.filter(F.year('invoicedate') >= 2011)
    # Create a monthly invoice date column
    df_with_month = df_retail_2011.withColumn(
        'invoice_month', F.date_format('invoicedate', 'yyyy-MM'))

    # Find the previous invoice month for each customer
    window_spec = Window.partitionBy('customerid').orderBy('invoice_month')
    df_with_churn = df_with_month.withColumn(
        'prev_invoice_month',
        F.lag('invoice_month').over(window_spec)
    )
    df_with_churn = df_with_churn.withColumn(
        'month_diff',
        F.months_between('invoice_month', 'prev_invoice_month')
    )

    # Filter customers with gap more than one month between purchases
    df_churned = df_with_churn.filter(df_with_churn['month_diff'] > 1)

    # Find total of churned customers
    churn_counts = df_churned.groupBy('invoice_month').agg(
        F.countDistinct('customerid').alias('churned_customers')
    )

    # Find total customer at the start of each month / period
    total_customers_start = df_with_month.groupBy(
        'invoice_month').agg(
            F.countDistinct('customerid').alias('total_customers_start')
    )

    # Join churned customer counts and total customer counts by month
    df_churn_rate = churn_counts.join(
        total_customers_start,
        'invoice_month',
        'outer'
    )

    # Since not all month has churned customers
    # which will give null value after joined, change the null to 0
    df_churn_rate = df_churn_rate.fillna(0, subset=['churned_customers'])

    # Find churn rate
    df_churn_rate = df_churn_rate.withColumn(
        'churn_rate',
        F.round((F.col('churned_customers') / F.col('total_customers_start')) * 100, 2)
    )

    return df_churn_rate
