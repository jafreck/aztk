import argparse
import typing

import aztk.gatk
from aztk_cli import config
from aztk_cli.config import SshConfig

from . import init
from .cluster import cluster
from .job import job


def setup_parser(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(
        title="Actions", dest="action", metavar="<action>")
    subparsers.required = True

    cluster_parser = subparsers.add_parser(
        "cluster", help="Commands to manage a cluster")
    job_parser = subparsers.add_parser(
        "job", help="Commands to manage a Job")
    init_parser = subparsers.add_parser(
        "init", help="Initialize your environment")
    
    cluster.setup_parser(cluster_parser)
    job.setup_parser(job_parser)
    init.setup_parser(init_parser)

    # parser.add_argument('command', nargs='*',
    #                     help='The gatk command to run')

def execute(args: typing.NamedTuple):
    actions = dict(
        cluster=cluster.execute,
        job=job.execute,
        init=init.execute
    )
    func = actions[args.action]
    func(args)

#     if args.command:
#         execute_command(args.command)


# def execute_command(command):
#     gatk_client = aztk.gatk.Client(config.load_aztk_secrets())
#     from aztk.utils.ssh import node_exec_command


#     cluster = gatk_client.get_cluster(args.cluster_id)
#     configuration = gatk_client.get_cluster_config(args.cluster_id)

#     master_node_id = cluster.master_node_id

#     ssh_conf = SshConfig()

#     ssh_conf.merge(
#         cluster_id=args.cluster_id,
#         username=args.username,
#         job_ui_port=None,
#         job_history_ui_port=None,
#         web_ui_port=None,
#         host=None,
#         connect=None,
#         internal=None)

#     node_id, output = gatk_client.node_run(
#         cluster_id=args.cluster_id,
#         node_id=master_node_id,
#         command="docker exec gatk /bin/bash -c 'source /root/.gatkbashrc; echo $PATH; gatk" + args.command,
#     )

#     print(output)
