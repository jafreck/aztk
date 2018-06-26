import concurrent.futures


def delete_user_on_pool(base_client, username, pool_id, nodes): #TODO: change from pool_id, nodes to cluster_id
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(base_client.delete_user_on_node, pool_id, node.id, username) for node in nodes]
        concurrent.futures.wait(futures)
