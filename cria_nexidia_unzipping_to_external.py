from datetime import timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSObjectToLocalOperator, GCSUnzipOperator, GCSDeleteObjectsOperator, GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryValueCheckOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup

# Define the DAG
dag = DAG(
    dag_id="gcs_unzip_dag",
    description="Unzips a zipped file in Google Cloud Storage.",
    schedule_interval="@once",
    catchup=False,
)

# Define the tasks
begin_task = DummyOperator(task_id="begin_task")
end_task = DummyOperator(task_id="end_task")

gcs_list_objects_task = GCSListObjectsOperator(
    task_id="gcs_list_objects_task",
    bucket="my-bucket",
    prefix="",
    gcp_conn_id="google_cloud_default",
)

gcs_object_to_local_task = GCSObjectToLocalOperator(
    task_id="gcs_object_to_local_task",
    bucket="my-bucket",
    object_name=gcs_list_objects_task.output["objects"][0]["name"],
    local_path="/tmp/my-file.zip",
    gcp_conn_id="google_cloud_default",
)

gcs_unzip_task = GCSUnzipOperator(
    task_id="gcs_unzip_task",
    bucket="my-bucket",
    object_name="/tmp/my-file.zip",
    destination_blob_name="my-unzipped-file",
    gcp_conn_id="google_cloud_default",
)

validate_file_exists_task = GCSObjectExistenceSensor(
    task_id="validate_file_exists_task", bucket=BUCKET_NAME, object=DATA_FILE_NAME
)

check_bq_row_count_task = BigQueryValueCheckOperator(
    task_id="check_row_count_task",
    sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
    pass_value=9,
    use_legacy_sql=False,
)

cleanup_task = GCSDeleteObjectsOperator(
    task_id="cleanup_task",
    bucket_name=GCS_BUCKET,
    prefix=f"{GCS_OBJECT_PATH}/{table}/",
    execution_timeout=timedelta(minutes=5),
)

# Add the tasks to the DAG
dag.add_operator(gcs_list_objects_task)
dag.add_operator(gcs_object_to_local_task)

