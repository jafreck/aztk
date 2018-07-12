import concurrent.futures


def delete_user_on_pool(base_client, id, nodes, username): #TODO: remove nodes param
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(base_client.delete_user_on_node, id, node.id, username) for node in nodes]
        concurrent.futures.wait(futures)
