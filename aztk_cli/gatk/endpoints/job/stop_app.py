import argparse
import time
import typing

import aztk.gatk
from aztk_cli import config, log, utils


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

    if gatk_client.stop_job_app(args.job_id, args.app_name):
        log.info("Stopped app {0}".format(args.app_name))
    else:
        log.error("App with name {0} does not exist or was already deleted")
