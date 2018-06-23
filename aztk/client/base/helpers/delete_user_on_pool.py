import concurrent.futures


def __delete_user_on_pool(base_client, username, pool_id, nodes):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(base_client.delete_user, pool_id, node.id, username) for node in nodes]
        concurrent.futures.wait(futures)
