from __future__ import print_function

import datetime
import time

import azure.batch.models as batch_models

from aztk.utils import constants


class MasterInvalidStateError(Exception):
    pass


def wait_for_master(core_operations, spark_operations, cluster_id: str):
    cluster = None
    master_node = None
    start_time = datetime.datetime.now()
    while True:
        delta = datetime.datetime.now() - start_time
        if delta.total_seconds() > constants.WAIT_FOR_MASTER_TIMEOUT:
            raise MasterInvalidStateError("Master didn't become ready before timeout.")

        cluster = spark_operations.get(cluster_id)

        if cluster.master_node_id:
            master_node = core_operations.get_node(cluster_id, cluster.master_node_id)
        if master_node:
            if master_node.state in [batch_models.ComputeNodeState.idle, batch_models.ComputeNodeState.running]:
                break
            if master_node.state is batch_models.ComputeNodeState.start_task_failed:
                raise MasterInvalidStateError("Start task failed on master")
            elif master_node.state in [batch_models.ComputeNodeState.unknown, batch_models.ComputeNodeState.unusable]:
                raise MasterInvalidStateError("Master is in an invalid state")

        time.sleep(5)
