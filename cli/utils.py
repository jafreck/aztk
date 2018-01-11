import getpass
import sys
import threading
import time
from subprocess import call
from typing import List
import azure.batch.models as batch_models
import aztk.spark
from aztk import error
from aztk.utils import get_ssh_key
from . import log


def get_ssh_key_or_prompt(ssh_key, username, password, secrets_config):
    ssh_key = get_ssh_key.get_user_public_key(ssh_key, secrets_config)

    if username is not None and password is None and ssh_key is None:
        for i in range(3):
            if i > 0:
                log.error("Please try again.")
            password = getpass.getpass("Please input a password for user '{0}': ".format(username))
            confirm_password = getpass.getpass("Please confirm your password for user '{0}': ".format(username))
            if password != confirm_password:
                log.error("Password confirmation did not match.")
            elif not password:
                log.error("Password cannot be empty.")
            else:
                break
        else:
            raise error.AztkError("Failed to get valid password, cannot add user to cluster. It is recommended that you provide a ssh public key in .aztk/secrets.yaml. Or provide an ssh-key or password with commnad line parameters (--ssh-key or --password). You may also run the 'aztk spark cluster add-user' command to add a user to this cluster.")
    return ssh_key, password

def print_cluster(client, cluster: aztk.spark.models.Cluster):
    node_count = __pretty_node_count(cluster)

    log.info("")
    log.info("Cluster         %s", cluster.id)
    log.info("------------------------------------------")
    log.info("State:          %s", cluster.visible_state)
    log.info("Node Size:      %s", cluster.vm_size)
    log.info("Nodes:          %s", node_count)
    log.info("| Dedicated:    %s", __pretty_dedicated_node_count(cluster))
    log.info("| Low priority: %s", __pretty_low_pri_node_count(cluster))
    log.info("")

    print_format = '{:<36}| {:<19} | {:<21}| {:<8}'
    print_format_underline = '{:-<36}|{:-<21}|{:-<22}|{:-<8}'
    log.info(print_format.format("Nodes", "State", "IP:Port", "Master"))
    log.info(print_format_underline.format('', '', '', ''))

    if not cluster.nodes:
        return
    for node in cluster.nodes:
        remote_login_settings = client.get_remote_login_settings(cluster.id, node.id)
        log.info(
            print_format.format(
                node.id,
                node.state.value,
                '{}:{}'.format(remote_login_settings.ip_address, remote_login_settings.port),
                '*' if node.id == cluster.master_node_id else '')
        )
    log.info('')

def __pretty_node_count(cluster: aztk.spark.models.Cluster) -> str:
    if cluster.pool.allocation_state is batch_models.AllocationState.resizing:
        return '{} -> {}'.format(
            cluster.total_current_nodes,
            cluster.total_target_nodes)
    else:
        return '{}'.format(cluster.total_current_nodes)

def __pretty_dedicated_node_count(cluster: aztk.spark.models.Cluster)-> str:
    if (cluster.pool.allocation_state is batch_models.AllocationState.resizing
            or cluster.pool.state is batch_models.PoolState.deleting)\
            and cluster.current_dedicated_nodes != cluster.target_dedicated_nodes:
        return '{} -> {}'.format(
            cluster.current_dedicated_nodes,
            cluster.target_dedicated_nodes)
    else:
        return '{}'.format(cluster.current_dedicated_nodes)

def __pretty_low_pri_node_count(cluster: aztk.spark.models.Cluster)-> str:
    if (cluster.pool.allocation_state is batch_models.AllocationState.resizing
            or cluster.pool.state is batch_models.PoolState.deleting)\
            and cluster.current_low_pri_nodes != cluster.target_low_pri_nodes:
        return '{} -> {}'.format(
            cluster.current_low_pri_nodes,
            cluster.target_low_pri_nodes)
    else:
        return '{}'.format(cluster.current_low_pri_nodes)

def print_clusters(clusters: List[aztk.spark.models.Cluster]):
    print_format = '{:<34}| {:<10}| {:<20}| {:<7}'
    print_format_underline = '{:-<34}|{:-<11}|{:-<21}|{:-<7}'

    log.info(print_format.format('Cluster', 'State', 'VM Size', 'Nodes'))
    log.info(print_format_underline.format('', '', '', ''))
    for cluster in clusters:
        node_count = __pretty_node_count(cluster)

        log.info(
            print_format.format(
                cluster.id,
                cluster.visible_state,
                cluster.vm_size,
                node_count
            )
        )

def stream_logs(client, cluster_id, application_name):
    current_bytes = 0
    while True:
        app_logs = client.get_application_log(
            cluster_id=cluster_id,
            application_name=application_name,
            tail=True,
            current_bytes=current_bytes)
        print(app_logs.log, end="")
        if app_logs.application_state == 'completed':
            break
        current_bytes = app_logs.total_bytes
        time.sleep(3)

def ssh_in_master(
        client,
        cluster_id: str,
        username: str = None,
        webui: str = None,
        jobui: str = None,
        jobhistoryui: str = None,
        jupyter: str = None,
        namenodeui: str = None,
        rstudioserver: str = None,
        ports=None,
        host: bool = False,
        connect: bool = True):
    """
        SSH into head node of spark-app
        :param cluster_id: Id of the cluster to ssh in
        :param username: Username to use to ssh
        :param webui: Port for the spark master web ui (Local port)
        :param jobui: Port for the job web ui (Local port)
        :param jupyter: Port for jupyter (Local port)
        :param rstudioserver: Port for rstudio server (Local port)
        :param ports: an list of local and remote ports
        :type ports: [[<local-port>, <remote-port>]]
    """

    # Get master node id from task (job and task are both named pool_id)
    cluster = client.get_cluster(cluster_id)
    master_node_id = cluster.master_node_id

    if master_node_id is None:
        raise aztk.error.ClusterNotReadyError("Master node has not yet been picked!")

    # get remote login settings for the user
    remote_login_settings = client.get_remote_login_settings(cluster.id, master_node_id)
    master_node_ip = remote_login_settings.ip_address
    master_node_port = remote_login_settings.port

    spark_web_ui_port = aztk.utils.constants.DOCKER_SPARK_WEB_UI_PORT
    spark_worker_ui_port = aztk.utils.constants.DOCKER_SPARK_WORKER_UI_PORT
    spark_rstudio_server_port = aztk.utils.constants.DOCKER_SPARK_RSTUDIO_SERVER_PORT
    spark_jupyter_port = aztk.utils.constants.DOCKER_SPARK_JUPYTER_PORT
    spark_job_ui_port = aztk.utils.constants.DOCKER_SPARK_JOB_UI_PORT
    spark_job_history_ui_port = aztk.utils.constants.DOCKER_SPARK_JOB_UI_HISTORY_PORT
    spark_namenode_ui_port = aztk.utils.constants.DOCKER_SPARK_NAMENODE_UI_PORT

    ssh_command = aztk.utils.command_builder.CommandBuilder('ssh')

    # get ssh private key path if specified
    ssh_priv_key = client.secrets_config.ssh_priv_key
    if ssh_priv_key is not None:
        ssh_command.add_option("-i", ssh_priv_key)

    ssh_command.add_argument("-t")
    ssh_command.add_option("-L", "{0}:localhost:{1}".format(
        webui,  spark_web_ui_port), enable=bool(webui))
    ssh_command.add_option("-L", "{0}:localhost:{1}".format(
        jobui, spark_job_ui_port), enable=bool(jobui))
    ssh_command.add_option("-L", "{0}:localhost:{1}".format(
        jobhistoryui, spark_job_history_ui_port), enable=bool(jobui))
    ssh_command.add_option("-L", "{0}:localhost:{1}".format(
        jupyter, spark_jupyter_port), enable=bool(jupyter))
    ssh_command.add_option("-L", "{0}:localhost:{1}".format(
        namenodeui, spark_namenode_ui_port), enable=bool(namenodeui))
    ssh_command.add_option("-L", "{0}:localhost:{1}".format(
        rstudioserver, spark_rstudio_server_port), enable=bool(rstudioserver))

    if ports is not None:
        for port in ports:
            ssh_command.add_option(
                "-L", "{0}:localhost:{1}".format(port[0], port[1]))

    user = username if username is not None else '<username>'
    ssh_command.add_argument(
        "{0}@{1} -p {2}".format(user, master_node_ip, master_node_port))

    if host is False:
        ssh_command.add_argument("\'sudo docker exec -it spark /bin/bash\'")

    command = ssh_command.to_str()

    if connect:
        call(command, shell=True)
    return '\n\t{}\n'.format(command)

def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.
    :param batch_exception:
    """
    log.error("-------------------------------------------")
    log.error("Exception encountered:")
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        log.error(batch_exception.error.message.value)
        if batch_exception.error.values:
            log.error('')
            for mesg in batch_exception.error.values:
                log.error("%s:\t%s", mesg.key, mesg.value)
    log.error("-------------------------------------------")


class Spinner:
    busy = False
    delay = 0.1

    @staticmethod
    def spinning_cursor():
        while 1: 
            for cursor in '|/-\\': yield cursor

    def __init__(self, delay=None):
        self.spinner_generator = self.spinning_cursor()
        if delay and float(delay): self.delay = delay

    def spinner_task(self):
        while self.busy:
            sys.stdout.write(next(self.spinner_generator))
            sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\b')
            sys.stdout.flush()

    def start(self):
        self.busy = True
        threading.Thread(target=self.spinner_task, daemon=True).start()

    def stop(self):
        self.busy = False
        time.sleep(self.delay)
