import concurrent.futures


def create_user_on_pool(base_client, username, pool_id, nodes, ssh_pub_key=None, password=None):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(base_client.create_user_on_node, username, pool_id, node.id, ssh_pub_key, password): node
            for node in nodes
        }
        concurrent.futures.wait(futures)
