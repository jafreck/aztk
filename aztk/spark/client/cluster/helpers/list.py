import azure.batch.models.batch_error as batch_error

import aztk.models    # TODO: get rid of this import and use aztk.spark.models
from aztk import error
from aztk.spark import models
from aztk.spark import helpers


def list_clusters(spark_cluster_client):
    try:
        return [models.Cluster(pool) for pool in spark_cluster_client.__list_clusters(aztk.models.Software.spark)]
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
