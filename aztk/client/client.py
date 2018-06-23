from aztk.spark.client.cluster import Client as cluster_client
from aztk.spark.client.job import Client as job_client

class Client:
    cluster = cluster_client
    job = job_client
