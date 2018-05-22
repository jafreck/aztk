import argparse
import typing

import aztk.spark
from aztk_cli import config, utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your spark cluster')
    parser.add_argument('--node-id',
                        dest='node_id',
                        required=True,
                        help='The unique id of the node in your cluster.')
    parser.add_argument('command',
                        help='The command to run on your spark cluster')
    parser.add_argument('--internal', action='store_true',
                        help='Connect using the local IP of the master node. Only use if using a VPN.')
    parser.set_defaults(internal=False)


def execute(args: typing.NamedTuple):
    spark_client = aztk.spark.Client(config.load_aztk_secrets())
    with utils.Spinner():
        node_id, output = spark_client.node_run(args.cluster_id, args.node_id, args.command, args.internal)
    print_execute_result(node_id, output)


def print_execute_result(node_id, result):
    print("-" * (len(node_id) + 6))
    print("| ", node_id, " |")
    print("-" * (len(node_id) + 6))
    if isinstance(result, Exception):
        print(result + "\n")
    else:
        for line in result:
            print(line)
