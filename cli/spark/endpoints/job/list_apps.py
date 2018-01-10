import argparse
import typing
from cli.spark.aztklib import load_spark_client
from cli import utils

def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your AZTK job')


def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()
    utils.print_applications(spark_client.list_applicaitons(args.job_id))
