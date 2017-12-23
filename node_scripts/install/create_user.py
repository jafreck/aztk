import os
import yaml
import azure.batch.models as batch_models
import azure.batch.models.batch_error as batch_error
from datetime import datetime, timezone, timedelta
'''
    Creates a user if the user configuration file at $DOCKER_WORKING_DIR/user.yaml exists
'''

def create_user(batch_client):
    path = os.path.join(os.environ['DOCKER_WORKING_DIR'], "user.yaml")
    
    if not os.path.isfile(path):
        print("No user to create.")
        return

    with open(path) as file:
        user_conf = yaml.load(file.read())
    

    print({'pool_id': os.environ['AZ_BATCH_POOL_ID'],
            'node_id': os.environ['AZ_BATCH_NODE_ID'],
            'user': batch_models.ComputeNodeUser(
                name=user_conf['username'],
                is_admin=True,
                password=user_conf['password'],
                ssh_public_key=user_conf['ssh-key'])})

    try:
        batch_client.compute_node.add_user(
            pool_id=os.environ['AZ_BATCH_POOL_ID'],
            node_id=os.environ['AZ_BATCH_NODE_ID'],
            user=batch_models.ComputeNodeUser(
                name=user_conf['username'],
                is_admin=True,
                password=user_conf['password'],
                ssh_public_key=str(user_conf['ssh-key']),
                expiry_time=datetime.now(timezone.utc) + timedelta(days=365)
            )
        )
    except batch_error.BatchErrorException as e:
        print(e)
