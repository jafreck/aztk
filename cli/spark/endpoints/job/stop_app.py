import argparse
import typing
import time
from cli.spark.aztklib import load_spark_client
from cli import utils
from cli import log


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your spark cluster')
    parser.add_argument('--name',
                        dest='app_name',
                        required=True,
                        help='The unique id of your job name')


def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()

    if spark_client.stop_job_app(args.job_id, args.app_name):
        log.info("Stopped app {0}".format(args.app_name))
    else:
        log.error("App with name {0} does not exist or was already deleted")
