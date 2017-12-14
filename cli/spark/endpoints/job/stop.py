import argparse
import typing
import time
from cli.spark.aztklib import load_spark_client
from cli import utils

def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your spark cluster')


def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()
    spark_client.stop_job(args.job_id)
    print("Stopped Job {0}".format(args.job_id))