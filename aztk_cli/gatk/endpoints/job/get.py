import argparse
import time
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

    utils.print_job(gatk_client, gatk_client.get_job(args.job_id))
