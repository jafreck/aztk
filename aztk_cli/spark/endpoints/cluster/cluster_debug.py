import argparse
import typing
import aztk.spark
from aztk_cli import config


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id', dest='cluster_id', required=True,
                        help='The unique id of your spark cluster')

    parser.add_argument('--output', '-o', required=True,
                        help='the path for the output folder')


def execute(args: typing.NamedTuple):
    spark_client = aztk.spark.Client(config.load_aztk_secrets())

    spark_client.run_cluster_diagnostics(cluster_id=args.cluster_id, path=args.output)
    # TODO: analyze results, display some info about status
