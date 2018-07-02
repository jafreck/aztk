import azure.batch.models.batch_error as batch_error

import aztk.models    # TODO: get rid of this import and use aztk.spark.models
from aztk import error
from aztk.spark import models
from aztk.utils import helpers


def list_clusters(spark_cluster_operations):
    try:
        software_metadata_key = aztk.models.Software.spark
        return [models.Cluster(pool) for pool in super(type(spark_cluster_operations), spark_cluster_operations).list(software_metadata_key)]
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
