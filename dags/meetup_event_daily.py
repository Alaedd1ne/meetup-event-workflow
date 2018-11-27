import os
from datetime import datetime
from datetime import timedelta

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from helpers.meetup_api_consumer import extractDailyData

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 10,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 5
}


export_dir = str(os.environ.get('DAGS_FOLDER')).replace('dags', 'data')
bucket_name=os.environ.get('GCS_BUCKET')
project_id=os.environ.get('GCP_PROJECT')
dataset_id='meetup'

# Config and describe the meetup_daily_events DAG
with DAG(dag_id='meetup_daily_events_V2',
         max_active_runs=3,
         concurrency=3,
         catchup=True,
         schedule_interval='@daily',
         default_args=default_args) as dag:

    # Calls the Meetup api the retrieve brute event data with keywords
    retrieve_meetup_daily_events = PythonOperator(
        task_id='retrieve_meetup_daily_events',
        python_callable=extractDailyData,
        params={'dir': export_dir},
        provide_context=True,
        pool='meetup_api_pool'
    )

    # Check if a new event export file is available in the gcs bucket
    def is_event_file_created(ds, **kwargs):
        gcs_hook = GoogleCloudStorageHook()
        event_file_uploaded_to_gcs = gcs_hook.exists(
            bucket=bucket_name.split("/")[-1],
            object='data/output_events_{date}.txt'.format(date=ds)
        )
        if event_file_uploaded_to_gcs is True:
            return 'load_events_data_to_bigquery'
        else:
            return 'done'

    # Operator for branching
    branching = BranchPythonOperator(
        task_id='branching_tasks',
        python_callable=is_event_file_created,
        provide_context=True,
        dag=dag
    )

    # Dummy operator
    done = DummyOperator(
        task_id='done',
        dag=dag
    )

    # Load the event file to Bigquery
    load_events_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='load_events_data_to_bigquery',
        bucket=bucket_name.split("/")[-1],
        source_objects=[
            'data/output_events_{date}.txt'.format(date="{{ ds }}")
        ],
        destination_project_dataset_table='meetup.event',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        schema_object='dags/schemas/event_schema.json'
    )

    # retrieve most used keywords
    query_most_used_keywords = BigQueryOperator(
        task_id='query_most_used_keywords',
        sql="""
            SELECT
              DISTINCT mentions.text.content AS Keyword,
              venue.city as City,
              venue.localized_country_name as Country,
              COUNT(*) AS count
            FROM
              `{project_id}.{dataset_id}.event`,
              UNNEST(key_word) AS key_word_1,
              UNNEST(key_word_1.mentions) AS mentions
            WHERE
              CHAR_LENGTH(mentions.text.content) > 2
              AND 
              DATE(TIMESTAMP_MILLIS(time)) = DATE('{date}')
            GROUP BY
              venue.city,
              venue.localized_country_name,
              Keyword
            ORDER BY
              count DESC
            """
            .format(project_id=project_id,
                   dataset_id=dataset_id,
                   date='{{ ds }}'),
        destination_dataset_table='{project_id}.{dataset_id}.daily_keyword${partition_date}'
            .format(project_id=project_id,
                    dataset_id=dataset_id,
                    partition_date='{{ ds_nodash }}'),
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False
    )

    # Set upstreams between dags
    retrieve_meetup_daily_events >> branching
    branching >> load_events_to_bigquery
    branching >> done
    load_events_to_bigquery >> query_most_used_keywords