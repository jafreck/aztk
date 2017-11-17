import os
import logging
import azure.batch.batch_service_client as batch
import azure.storage.blob as blob
import azure.batch.batch_auth as batchauth

batch_account_name = os.environ["AZ_BATCH_ACCOUNT_NAME"]
batch_account_key = os.environ["BATCH_ACCOUNT_KEY"]
batch_account_url = os.environ["BATCH_ACCOUNT_URL"]

storage_account_name = os.environ["STORAGE_ACCOUNT_NAME"]
storage_account_key = os.environ["STORAGE_ACCOUNT_KEY"]
storage_account_suffix = os.environ["STORAGE_ACCOUNT_SUFFIX"]

pool_id = os.environ["AZ_BATCH_POOL_ID"]
node_id = os.environ["AZ_BATCH_NODE_ID"]
is_dedicated = os.environ["AZ_BATCH_NODE_IS_DEDICATED"]

spark_web_ui_port = os.environ["SPARK_WEB_UI_PORT"]
spark_worker_ui_port = os.environ["SPARK_WORKER_UI_PORT"]
spark_jupyter_port = os.environ["SPARK_JUPYTER_PORT"]
spark_job_ui_port = os.environ["SPARK_JOB_UI_PORT"]

def get_batch_client() -> batch.BatchServiceClient:
    credentials = batchauth.SharedKeyCredentials(
        batch_account_name,
        batch_account_key)
    return batch.BatchServiceClient(credentials, base_url=batch_account_url)

def get_blob_client() -> blob.BlockBlobService:
    return blob.BlockBlobService(
        account_name=storage_account_name,
        account_key=storage_account_key,
        endpoint_suffix=storage_account_suffix)

batch_client = get_batch_client()
blob_client = get_blob_client()

logging.info("Pool id is %s", pool_id)
logging.info("Node id is %s", node_id)
logging.info("Batch account name %s", batch_account_name)
logging.info("Is dedicated %s", is_dedicated)
