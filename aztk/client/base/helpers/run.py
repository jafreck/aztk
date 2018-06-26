import asyncio

import aztk.models as models
from aztk.utils import ssh as ssh_lib

def cluster_run(base_client, cluster_id, command, internal, container_name=None, timeout=None):
    cluster = base_client.get(cluster_id)
    pool, nodes = cluster.pool, list(cluster.nodes)
    if internal:
        cluster_nodes = [(node, models.RemoteLogin(ip_address=node.ip_address, port="22")) for node in nodes]
    else:
        cluster_nodes = [(node, base_client.get_remote_login_settings(pool.id, node.id)) for node in nodes]

    try:
        generated_username, ssh_key = base_client.generate_user_on_pool(pool.id, nodes)
        output = asyncio.get_event_loop().run_until_complete(
            ssh_lib.clus_exec_command(
                command,
                generated_username,
                cluster_nodes,
                ssh_key=ssh_key.exportKey().decode('utf-8'),
                container_name=container_name,
                timeout=timeout
            )
        )
        return output
    except OSError as exc:
        raise exc
    finally:
        base_client.delete_user_on_pool(generated_username, pool.id, nodes)
