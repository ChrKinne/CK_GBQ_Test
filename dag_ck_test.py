from datetime import datetime, timedelta

from google.cloud import bigquery
from google.cloud import storage

from google.cloud.bigquery.job import QueryJobConfig
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
# We need to import the bigquery operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "Test",
    "depends_on_past": False,
    "start_date": datetime.today() - timedelta(1),
    "email": "business.intelligence@springernature.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

ENVIRONMENTS = {
    'dev': {
        'dag-name': 'DAG_CK_GBQ_TEST',
        'connection-id': 'springer'
    }
}

# Interval of DAG
schedule_interval = '0 8 * * *'

# Global variables
bqproject = 'usage-data-reporting'
datasetenv = 'DEV'


# Create DAG
dag = DAG(
    ENVIRONMENTS['dev']['dag-name'],
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    description='CK DAG GBQ Test'
)




MOVE_LDZ_DATA_TO_DWH = BigQueryOperator(
    dag = dag,
    task_id='CK_GBQ_TEST_TASK_01',
    sql='Tests/ckgbqtest.sql',
    params={"project": bqproject, "environment": datasetenv},
    # destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_GBQ_Test.CK_GBQ_Test', # Zieltabelle
    write_disposition='WRITE_APPEND',  # Specify the write disposition truncate or write_append
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

# Referenziert auf den dag - name (s.o.) - liefert eine Beschreibung mit.
MOVE_LDZ_DATA_TO_DWH.doc_md = """Write data from LDZ to DWH"""

# Define how the different steps in the workflow are executed

MOVE_LDZ_DATA_TO_DWH

