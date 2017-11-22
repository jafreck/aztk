import os
import logging
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth

account_name = os.environ["AZ_BATCH_ACCOUNT_NAME"]
account_key = os.environ["BATCH_ACCOUNT_KEY"]
account_url = os.environ["BATCH_ACCOUNT_URL"]

pool_id = os.environ["AZ_BATCH_POOL_ID"]
node_id = os.environ["AZ_BATCH_NODE_ID"]
is_dedicated = os.environ["AZ_BATCH_NODE_IS_DEDICATED"]

def get_client() -> batch.BatchServiceClient:
    credentials = batchauth.SharedKeyCredentials(
        account_name,
        account_key)
    return batch.BatchServiceClient(credentials, base_url=account_url)

batch_client = get_client()

logging.info("Pool id is %s", pool_id)
logging.info("Node id is %s", node_id)
logging.info("Batch account name %s", account_name)
logging.info("Is dedicated %s", is_dedicated)
