import argparse
import typing

import azure.batch.models.batch_error as batch_error

import aztk
from aztk.models import ClusterConfiguration
from aztk_cli import config, log, utils
from aztk_cli.config import SshConfig

from aztk.spark.models import PortForwardingSpecification


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id', dest="cluster_id", help='The unique id of your spark cluster')
    parser.add_argument('--webui', help='Local port to port spark\'s master UI to')
    parser.add_argument('--jobui', help='Local port to port spark\'s job UI to')
    parser.add_argument('--jobhistoryui', help='Local port to port spark\'s job history UI to')
    parser.add_argument('-u', '--username', help='Username to spark cluster')
    parser.add_argument('--password', help='Password for the specified ssh user')
    parser.add_argument('--host', dest="host", action='store_true', help='Connect to the host of the Spark container')
    parser.add_argument('--no-connect', dest="connect", action='store_false',
                        help='Do not create the ssh session. Only print out the command to run.')
    parser.add_argument('--internal', action='store_true',
                        help='Connect using the local IP of the master node. Only use if using a VPN.')
    parser.set_defaults(connect=True, internal=False)


http_prefix = 'http://localhost:'


def execute(args: typing.NamedTuple):
    spark_client = aztk.spark.Client(config.load_aztk_secrets())
    cluster = spark_client.get_cluster(args.cluster_id)
    cluster_config = spark_client.get_cluster_config(args.cluster_id)
    ssh_conf = SshConfig()

    ssh_conf.merge(
        cluster_id=args.cluster_id,
        username=args.username,
        job_ui_port=args.jobui,
        job_history_ui_port=args.jobhistoryui,
        web_ui_port=args.webui,
        host=args.host,
        connect=args.connect,
        internal=args.internal)

    log.info("-------------------------------------------")
    utils.log_property("spark cluster id", ssh_conf.cluster_id)
    utils.log_property("open webui", "{0}{1}".format(http_prefix, ssh_conf.web_ui_port))
    utils.log_property("open jobui", "{0}{1}".format(http_prefix, ssh_conf.job_ui_port))
    utils.log_property("open jobhistoryui", "{0}{1}".format(http_prefix, ssh_conf.job_history_ui_port))
    print_plugin_ports(cluster_config)
    utils.log_property("ssh username", ssh_conf.username)
    utils.log_property("connect", ssh_conf.connect)
    log.info("-------------------------------------------")

    try:
        shell_out_ssh(spark_client, ssh_conf)
    except OSError:
        # no ssh client is found, falling back to pure python
        native_python_ssh_into_master(spark_client, cluster, ssh_conf.username, args.password)


def print_plugin_ports(cluster_config: ClusterConfiguration):
    if cluster_config and cluster_config.plugins:
        plugins = cluster_config.plugins
        has_ports = False
        plugin_ports = {}
        for plugin in plugins:
            plugin_ports[plugin.name] = []
            for port in plugin.ports:
                if port.expose_publicly:
                    has_ports = True
                    plugin_ports[plugin.name].append(port)
        
        if has_ports:
            log.info("plugins:")

        for plugin in plugin_ports:
            if plugin_ports[plugin]:
                log.info("  " + plugin)
                for port in plugin_ports[plugin]:
                    label = "    - open"
                    if port.name:
                        label += " {}".format(port.name)
                    url = "{0}{1}".format(http_prefix, port.public_port)
                    utils.log_property(label, url)


def native_python_ssh_into_master(spark_client, cluster, username, password):
    configuration = spark_client.get_cluster_config(cluster.id)
    plugin_ports = []
    if configuration and configuration.plugins:
        ports = [
            PortForwardingSpecification(
                port.internal,
                port.public_port) for plugin in configuration.plugins for port in plugin.ports if port.expose_publicly
        ]
        plugin_ports.extend(ports)

    spark_client.cluster_ssh_into_master(
        cluster.id,
        cluster.master_node_id,
        username,
        ssh_key=None,
        password=password,
        port_forward_list=[
            PortForwardingSpecification(remote_port=8080, local_port=8080),      # web ui
            PortForwardingSpecification(remote_port=4040, local_port=4040),      # job ui
            PortForwardingSpecification(remote_port=18080, local_port=18080),    # job history ui
        ] + plugin_ports
    )


def shell_out_ssh(spark_client, ssh_conf):
    try:
        ssh_cmd = utils.ssh_in_master(
            client=spark_client,
            cluster_id=ssh_conf.cluster_id,
            webui=ssh_conf.web_ui_port,
            jobui=ssh_conf.job_ui_port,
            jobhistoryui=ssh_conf.job_history_ui_port,
            username=ssh_conf.username,
            host=ssh_conf.host,
            connect=ssh_conf.connect,
            internal=ssh_conf.internal)

        if not ssh_conf.connect:
            log.info("")
            log.info("Use the following command to connect to your spark head node:")
            log.info("\t%s", ssh_cmd)

    except batch_error.BatchErrorException as e:
        if e.error.code == "PoolNotFound":
            raise aztk.error.AztkError("The cluster you are trying to connect to does not exist.")
        else:
            raise
