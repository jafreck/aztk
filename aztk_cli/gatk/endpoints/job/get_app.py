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
    parser.add_argument('--name',
                        dest='app_name',
                        required=True,
                        help='The unique id of your job name')


def execute(args: typing.NamedTuple):
    gatk_client = aztk.gatk.Client(config.load_aztk_secrets())

    utils.print_application(gatk_client.get_application(args.job_id, args.app_name))
