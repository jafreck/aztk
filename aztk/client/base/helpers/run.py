import asyncio

import aztk.models as models
from aztk.utils import batch_error_manager
from aztk.utils import ssh as ssh_lib


def cluster_run(base_operations, cluster_id, command, internal, container_name=None, timeout=None):
    cluster = base_operations.get(cluster_id)
    pool, nodes = cluster.pool, list(cluster.nodes)
    if internal:
        cluster_nodes = [(node, models.RemoteLogin(ip_address=node.ip_address, port="22")) for node in nodes]
    else:
        cluster_nodes = [(node, base_operations.get_remote_login_settings(cluster_id, node.id)) for node in nodes]
    with batch_error_manager():
        generated_username, ssh_key = base_operations.generate_user_on_cluster(pool.id, nodes)

    try:
        output = asyncio.get_event_loop().run_until_complete(
            ssh_lib.clus_exec_command(
                command,
                generated_username,
                cluster_nodes,
                ssh_key=ssh_key.exportKey().decode("utf-8"),
                container_name=container_name,
                timeout=timeout,
            ))
        return output
    except OSError as exc:
        raise exc
    finally:
        base_operations.delete_user_on_cluster(pool.id, nodes, generated_username)
