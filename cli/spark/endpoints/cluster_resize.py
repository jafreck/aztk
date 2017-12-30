import argparse
import typing
from cli.spark.aztklib import load_spark_client
from cli import utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your spark cluster')
    parser.add_argument('--size', type=int,
                            help='Number of vms in your cluster')
    parser.add_argument('--size-low-pri', type=int,
                            help='Number of low priority vms in your cluster')

def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()
    spark_client.resize_cluster(args.cluster_id, args.size, args.size_low_pri)
