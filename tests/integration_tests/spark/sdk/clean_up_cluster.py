import azure.batch.models as batch_models
from azure.batch.models import BatchErrorException, ComputeNodeState

from aztk.error import AztkError


def clean_up_cluster(spark_client, id):
    try:
        cluster = spark_client.cluster.get(id)
        nodes = [node for node in cluster.nodes]
        dont_delete_states = [ComputeNodeState.unusable, ComputeNodeState.start_task_failed]
        if not any([node.state in dont_delete_states for node in nodes]):
            spark_client.cluster.delete(id=id)
    except (BatchErrorException, AztkError) as e:
        pass
