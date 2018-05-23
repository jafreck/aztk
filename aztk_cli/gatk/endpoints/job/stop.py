import argparse
import time
import typing

import aztk.gatk
from aztk_cli import config, utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your AZTK job')


def execute(args: typing.NamedTuple):
    gatk_client = aztk.gatk.Client(config.load_aztk_secrets())
    gatk_client.stop_job(args.job_id)
    print("Stopped Job {0}".format(args.job_id))
