import argparse
import typing
import aztk.spark
from aztk_cli import config


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id', dest='cluster_id', required=True,
                        help='The unique id of your spark cluster')

    parser.add_argument('--output', required=False,
                        help='the path for the output folder')


def execute(args: typing.NamedTuple):
    spark_client = aztk.spark.Client(config.load_aztk_secrets())

    output = spark_client.run_cluster_diagnostics(cluster_id=args.cluster_id)
    print("cluster_debug_output")
    print(type(output))
    print(output)
    print("cluster_debug_output end")
