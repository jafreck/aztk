import os

from azure.batch.models import batch_error

from aztk import error
from aztk.utils import helpers


def _run(spark_cluster_operations, cluster_id, output_directory=None, brief=False):
    # copy debug program to each node
    output = spark_cluster_operations.copy(
        cluster_id, os.path.abspath("./aztk/spark/utils/debug.py"), "/tmp/debug.py", host=True)
    ssh_cmd = _build_diagnostic_ssh_command(brief)
    run_output = spark_cluster_operations.run(cluster_id, ssh_cmd, host=True)
    remote_path = "/tmp/debug.zip"
    if output_directory:
        output = spark_cluster_operations.download(cluster_id, remote_path, host=True)
        os.makedirs(output_directory)
        for node_output in output:
            node_output.output.seek(0)
            new_zip = os.path.join(output_directory, node_output.id + ".zip")
            stream = open(new_zip, 'wb+')
            stream.write(node_output.output.read())
        # write run output to debug/ directory
        with open(os.path.join(output_directory, "debug-output.txt"), 'w', encoding="UTF-8") as f:
            [f.write(node_output.output + '\n') for node_output in run_output]
    else:
        output = spark_cluster_operations.download(cluster_id, remote_path, host=True)

    return output


def _build_diagnostic_ssh_command(brief):
    return "sudo rm -rf /tmp/debug.zip; "\
           "sudo apt-get install -y python3-pip; "\
           "sudo -H pip3 install --upgrade pip; "\
           "sudo -H pip3 install docker; "\
           "sudo python3 /tmp/debug.py {}".format(brief)


def run_cluster_diagnostics(spark_cluster_operations, cluster_id, output_directory=None, brief=False):
    try:
        output = _run(spark_cluster_operations, cluster_id, output_directory, brief)
        return output
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
