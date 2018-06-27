import concurrent.futures

from Cryptodome.PublicKey import RSA

from aztk.utils import secure_utils


def generate_user_on_pool(base_client, pool_id, nodes):
    generated_username = secure_utils.generate_random_string()
    ssh_key = RSA.generate(2048)
    ssh_pub_key = ssh_key.publickey().exportKey('OpenSSH').decode('utf-8')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(base_client.create_user_on_node, generated_username, pool_id, node.id, ssh_pub_key): node
            for node in nodes
        }
        concurrent.futures.wait(futures)

    return generated_username, ssh_key