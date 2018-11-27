# Create the cloud composer ENV
gcloud beta composer environments create meetup-events-workflow \
    --project sfeir-innovation \
    --location europe-west1 \
    --zone europe-west1-b \
    --image-version=composer-1.3.0-airflow-1.10.0 \
    --machine-type=n1-standard-1 \
    --labels env=beta \
    --node-count=3 \
    --disk-size=20 \
    --python-version=3


# Add airflow connexion id
gcloud beta composer environments run meetup-events-workflow --help
gcloud beta composer environments run meetup-events-workflow --location=europe-west1 connections -- -a --conn_id="airflow_service_account_conn" --conn_type=google_cloud_platform --conn_extra='{ "extra__google_cloud_platform__project": "'"sfeir-innovation"'", "extra__google_cloud_platform__key_path":"'"/Users/alaeddinebouattour/GitRepo/meetup-stream-dataflow/dags/resources/KEY_FILE.json"'", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
    # The best to do is to put the json content to 'key json' field in connections
    # OR
gcloud beta composer environments run meetup-events-workflow --location=europe-west1 connections -- -a --conn_id="airflow_service_account_conn" --conn_type=google_cloud_platform --conn_extra='{ "extra__google_cloud_platform__project": "'"sfeir-innovation"'", "extra__google_cloud_platform__key_path":"'"/home/airflow/gcs/dags/resources/KEY_FILE.json"'", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'


# List dags
gcloud beta composer environments run meetup-events-workflow --location=europe-west1 list_dags

# Get dependencies list
pip freeze > requirements.txt
pip freeze | tr A-Z a-z > requirements.txt

# Install dependencies
gcloud composer environments update meetup-events-workflow \
    --update-pypi-packages-from-file requirements_effectif.txt \
    --location=europe-west1
                # Dependencies must be lower-case


# Update env variable
gcloud composer environments update meetup-events-workflow \
  --location=europe-west1 \
  --update-env-variables=DAGS_FOLDER=/home/airflow/gcs/dags,GCS_BUCKET=gs://europe-west1-meetup-events--cdcc1363-bucket


  export_dir = '{}/data/'.format(os.path.abspath(os.path.join(DAGS_FOLDER, os.pardir)))


# Create tables
bq mk --project_id=sfeir-innovation --schema=./dags/schemas/keywords_schema.json --time_partitioning_type=DAY -t meetup.keywords
bq mk --project_id=sfeir-innovation --schema=./dags/schemas/events_schema.json -t meetup.events

# Test event export task
gcloud composer environments run meetup-events-workflow \
          --location=europe-west1 \
          test -- meetup_daily_events \
           retrieve_meetup_daily_events '2018-11-11'

# RQ :::
Il faut créer un pool spécial pour la récupération des events api.. limited to 1 // event.

# Add pool : meetup_api_pool
airflow pool -s 1 meetup_api_pool

gcloud composer environments run meetup-events-workflow \
          --location=europe-west1 \
          pool -- -s meetup_api_pool 1 'meetup api task usage'

gcloud composer environments run meetup-events-workflow \
          --location=europe-west1 \
            trigger_dag -- help

# Trigger dag
gcloud composer environments run meetup-events-workflow \
          --location=europe-west1 \
          trigger_dag -- meetup_daily_events \
          -e '2018-11-10' \
          -r meetup_daily_events_2018-11-10
