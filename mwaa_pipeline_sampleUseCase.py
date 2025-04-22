from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum
from pytz import timezone
from datetime import datetime
import boto3


# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("Asia/Kolkata")        # This is to set the pipline local reporting time in IST.


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False,
}

#DAG instance instantiation
with DAG(
    'data-refresh-pipeline',
    default_args=default_args,
    description='Invoking Lambda function & glue job subsequently for tables refresh in the datalake',
    schedule_interval='0 10 * * 1-5',                              #Runs every weekday day at 10:00 AM IST.
    start_date=datetime(2025,3,31,0,0,tzinfo=local_tz),
    catchup=False,
    tags=['lambda', 'glue', 'orchestration'],
) as dag:

    #Invoking Lambda function
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id='lambda_function_invokation',
        function_name='<name of AWS Lambda functions>',
        payload='{}',
        aws_conn_id='aws_services', # name of default AWS connection being made.
        log_type='Tail',
        retry_exponential_backoff=True,
    )

    #Triggering Glue job on Lambda success
    trigger_glue_job = GlueJobOperator(
        task_id='glue_job_triggering',
        job_name='<Glue ETL job name>',
        script_location='<S3 location where .py files glue ETL job is kept>',
        create_job_kwargs={}, # Populate only if you're creating a new glue job when this DAG runs.
        iam_role_name='<iam role of Glue job>',
        region_name="ap-south-1", # it could be any region where AWS account is enabled
        verbose=True,  # keep it true if extensive logs are needed, this would be useful to debug
        aws_conn_id='aws_services',
        dag=dag,






    # setting up orchestration pipeline
    invoke_lambda >> trigger_glue_job
