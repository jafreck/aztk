import os
from datetime import datetime, timedelta, timezone

import azure.batch.models as batch_models
import yaml
from azure.batch.models import BatchErrorException
from Cryptodome.Cipher import AES, PKCS1_OAEP
from Cryptodome.PublicKey import RSA

from aztk.node_scripts.core import config
"""
    Creates a user if the user configuration file at $AZTK_WORKING_DIR/user.yaml exists
"""


def create_user(batch_client):
    path = os.path.join(config.aztk_working_dir, "user.yaml")

    if not os.path.isfile(path):
        print("No user to create.")
        return

    with open(path, "r", encoding="UTF-8") as file:
        user_conf = yaml.load(file.read())

    try:
        password = None if user_conf["ssh-key"] else decrypt_password(user_conf)

        batch_client.compute_node.add_user(
            pool_id=config.pool_id,
            node_id=config.node_id,
            user=batch_models.ComputeNodeUser(
                name=user_conf["username"],
                is_admin=True,
                password=password,
                ssh_public_key=str(user_conf["ssh-key"]),
                expiry_time=datetime.now(timezone.utc) + timedelta(days=365),
            ),
        )
    except BatchErrorException as e:
        print(e)


def decrypt_password(user_conf):
    cipher_text = user_conf["password"]
    encrypted_aes_session_key = user_conf["aes_session_key"]
    cipher_aes_nonce = user_conf["cipher_aes_nonce"]
    tag = user_conf["tag"]

    # Read private key
    with open(os.path.join(config.aztk_working_dir, "id_rsa"), encoding="UTF-8") as f:
        private_key = RSA.import_key(f.read())
    # Decrypt the session key with the public RSA key
    cipher_rsa = PKCS1_OAEP.new(private_key)
    session_key = cipher_rsa.decrypt(encrypted_aes_session_key)

    # Decrypt the data with the AES session key
    cipher_aes = AES.new(session_key, AES.MODE_EAX, cipher_aes_nonce)
    password = cipher_aes.decrypt_and_verify(cipher_text, tag)
    return password.decode("utf-8")
