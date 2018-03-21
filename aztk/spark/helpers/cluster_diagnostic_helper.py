import os
from aztk.utils import ssh
from aztk.utils.command_builder import CommandBuilder
from aztk import models as aztk_models
import azure.batch.models as batch_models

def run(spark_client, cluster_id):
    # build ssh command to run on each node
    ssh_cmd = _build_diagnostic_ssh_command()
    output = spark_client.cluster_run(cluster_id, ssh_cmd, host=True)
    print(output)
    # copy the output on each node back to the local machine
    local_path = os.path.abspath("./tmp/debug.zip")
    remote_path = "/tmp/debug.zip"
    output = spark_client.cluster_copy(cluster_id, remote_path, local_path, host=True, get=True)

    # zip it all up into one folder

    return output


def _build_diagnostic_ssh_command():
    ssh_cmd = "echo $HOSTNAME; "\
              "sudo rm -rf /tmp/debug; "\
              "sudo rm -rf /tmp/debug.zip; "\
              "mkdir /tmp/debug; "\
              "echo $HOSTNAME > /tmp/debug/hostname.txt; "\
              "sudo apt-get -y install zip; "\
              "df -h > /tmp/debug/df.txt; "\
              "sudo zip -r /tmp/debug.zip /tmp/debug/; "\
              "sudo chmod 777 /tmp/debug.zip"
    print(ssh_cmd)

    return ssh_cmd