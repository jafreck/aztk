import asyncio

import azure.batch.models.batch_error as batch_error

import aztk.models as models
from aztk.utils import ssh as ssh_lib


def cluster_copy(cluster_client, cluster_id, source_path, destination_path=None, container_name=None, internal=False, get=False, timeout=None):
    pool, nodes = cluster_client.__get_pool_details(cluster_id)
    nodes = list(nodes)
    if internal:
        cluster_nodes = [(node, models.RemoteLogin(ip_address=node.ip_address, port="22")) for node in nodes]
    else:
        cluster_nodes = [(node, cluster_client.__get_remote_login_settings(pool.id, node.id)) for node in nodes]

    try:
        generated_username, ssh_key = cluster_client.generate_user_on_pool(pool.id, nodes)
        output = asyncio.get_event_loop().run_until_complete(
            ssh_lib.clus_copy(
                container_name=container_name,
                username=generated_username,
                nodes=cluster_nodes,
                source_path=source_path,
                destination_path=destination_path,
                ssh_key=ssh_key.exportKey().decode('utf-8'),
                get=get,
                timeout=timeout
            )
        )
        return output
    except (OSError, batch_error.BatchErrorException) as exc:
        raise exc
    finally:
        cluster_client.__delete_user_on_pool(generated_username, pool.id, nodes)
