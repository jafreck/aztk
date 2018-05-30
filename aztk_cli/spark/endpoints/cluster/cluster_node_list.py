import argparse
import typing

import aztk
from aztk_cli import config, log, utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your spark cluster')
    parser.add_argument('--internal', action='store_true',
                        help="Show the local IP of the nodes. "\
                             "Only use if using connecting with a VPN.")
    parser.add_argument('--quiet', '-q', action='store_true',
                        help="Show the local IP of the nodes. "\
                             "Only use if using connecting with a VPN.")
    parser.add_argument('--master', '-m', action='store_true',
                        help="Show the local IP of the nodes. "\
                             "Only use if using connecting with a VPN.")
    parser.set_defaults(internal=False)


def execute(args: typing.NamedTuple):
    spark_client = aztk.spark.Client(config.load_aztk_secrets())
    cluster_id = args.cluster_id
    cluster = spark_client.get_cluster(cluster_id)
    if args.quiet:
        if args.master:
            utils.print_master_node_id(spark_client, cluster)
        else:
            print(cluster.master_node_id)
    else:
        utils.print_nodes(spark_client, cluster, args.internal)
