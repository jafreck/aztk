import os
from aztk.utils import ssh
from aztk.utils.command_builder import CommandBuilder
from aztk import models as aztk_models
import azure.batch.models as batch_models

def run(spark_client, cluster_id, path):
    # copy debug program to each node
    spark_client.cluster_copy(cluster_id, os.path.abspath("./aztk/spark/utils/debug.py"), "/tmp/debug.py", host=True)
    ssh_cmd = _build_diagnostic_ssh_command()
    output = spark_client.cluster_run(cluster_id, ssh_cmd, host=True)
    # local_path = os.path.abspath(path)
    remote_path = "/tmp/debug.zip"
    output = spark_client.cluster_copy(cluster_id, remote_path, path, host=True, get=True)
    return output


def _build_diagnostic_ssh_command():
    return "sudo rm -rf /tmp/debug.zip; "\
           "sudo apt-get install -y python3-pip; "\
           "pip3 install --upgrade pip; "\
           "pip3 install docker; "\
           "sudo python3 /tmp/debug.py 2>&1 > /tmp/debug-output.txt"
