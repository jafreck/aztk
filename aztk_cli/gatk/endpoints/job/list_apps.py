import argparse
import typing

import aztk.spark
from aztk_cli import config, utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your AZTK job')


def execute(args: typing.NamedTuple):
    gatk_client = aztk.spark.Client(config.load_aztk_secrets())
    utils.print_applications(gatk_client.list_applications(args.job_id))