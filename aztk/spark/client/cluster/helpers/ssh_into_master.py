from aztk.utils import batch_error_manager


def ssh_into_master(
        spark_cluster_operations,
        core_cluster_operations,
        cluster_id,
        username,
        ssh_key=None,
        password=None,
        port_forward_list=None,
        internal=False,
):
    with batch_error_manager():
        master_node_id = spark_cluster_operations.get(cluster_id).master_node_id
        core_cluster_operations.ssh_into_node(cluster_id, master_node_id, username, ssh_key, password,
                                              port_forward_list, internal)
