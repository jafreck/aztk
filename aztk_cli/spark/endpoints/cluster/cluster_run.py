import argparse
import typing

import aztk.spark
from aztk_cli import config, utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your spark cluster')
    parser.add_argument('command',
                        help='The command to run on your spark cluster')
    parser.add_argument('--internal', action='store_true',
                        help='Connect using the local IP of the master node. Only use if using a VPN.')
    parser.set_defaults(internal=False)


def execute(args: typing.NamedTuple):
    spark_client = aztk.spark.Client(config.load_aztk_secrets())
    results = spark_client.cluster_run(args.cluster_id, args.command, args.internal)
    for result in results:
        print("---------------------------") #TODO: replace with nodename
        for line in result:
            print(line)
    #TODO: pretty print result
