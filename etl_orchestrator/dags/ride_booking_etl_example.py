"""
Example DAG: Ride Booking ETL
This DAG demonstrates the bronze-silver-gold ETL pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Default arguments for all tasks
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'ride_booking_etl_example',
    default_args=default_args,
    description='Example ETL pipeline for ride booking data',
    schedule='0 2 * * *',  # Run daily at 2 AM (cron format)
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=['example', 'etl', 'ride-booking'],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Bronze layer tasks (raw data ingestion)
    def extract_bronze(**context):
        """Extract raw data to bronze layer"""
        execution_date = context['ds']
        print(f"Extracting raw data for {execution_date}")
        # TODO: Implement actual extraction logic
        # from app.etl.tasks.bronze import ...
        return "Bronze extraction complete"
    
    bronze_extraction = PythonOperator(
        task_id='bronze_extraction',
        python_callable=extract_bronze,
    )
    
    # Silver layer tasks (data cleaning and transformation)
    def transform_silver(**context):
        """Transform and clean data for silver layer"""
        execution_date = context['ds']
        print(f"Transforming data for {execution_date}")
        # TODO: Implement actual transformation logic
        # from app.etl.tasks.silver import ...
        return "Silver transformation complete"
    
    silver_transformation = PythonOperator(
        task_id='silver_transformation',
        python_callable=transform_silver,
    )
    
    # Gold layer tasks (aggregations and analytics)
    def create_gold_analytics(**context):
        """Create analytics tables in gold layer"""
        execution_date = context['ds']
        print(f"Creating analytics for {execution_date}")
        # TODO: Implement actual analytics logic
        # from app.etl.tasks.gold import ...
        return "Gold analytics complete"
    
    gold_analytics = PythonOperator(
        task_id='gold_analytics',
        python_callable=create_gold_analytics,
    )
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    start >> bronze_extraction >> silver_transformation >> gold_analytics >> end
