from typing import List

from azure.batch.models import BatchErrorException

from aztk import error
from aztk import models as base_models
from aztk.client import CoreClient
from aztk.spark import models
from aztk.spark.client.cluster import ClusterOperations
from aztk.spark.client.job import JobOperations
from aztk.spark.helpers import job_submission as job_submit_helper
from aztk.spark.utils import util
from aztk.utils import deprecate, deprecated, helpers


class Client(CoreClient):
    """The client used to create and manage Spark clusters

        Attributes:
            cluster (:obj:`aztk.spark.client.cluster.ClusterOperations`): Cluster
            job (:obj:`aztk.spark.client.job.JobOperations`): Job
    """

    def __init__(self, secrets_configuration: models.SecretsConfiguration):
        super().__init__()
        context = self._get_context(secrets_configuration)
        self.cluster = ClusterOperations(context)
        self.job = JobOperations(context)
