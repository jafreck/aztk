import argparse
import typing
from cli.spark.aztklib import load_spark_client
from cli import utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your spark cluster')
    parser.add_argument('command',
                        help='The command to run on your spark cluster')

def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()
    result = spark_client.cluster_run(args.cluster_id, args.command)
    #TODO: pretty print result
