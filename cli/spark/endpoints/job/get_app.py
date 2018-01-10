import argparse
import typing
import time
from cli.spark.aztklib import load_spark_client
from cli import utils

def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your AZTK job')
    parser.add_argument('--name',
                        dest='app_name',
                        required=True,
                        help='The unique id of your job name')


def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()

    utils.print_application(spark_client.get_application(args.job_id, args.app_name))
