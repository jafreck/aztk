import argparse
import time
import typing

import aztk.gatk
from aztk_cli import config, utils


def setup_parser(_: argparse.ArgumentParser):
    # No arguments for list yet
    pass

def execute(args: typing.NamedTuple):
    gatk_client = aztk.gatk.Client(config.load_aztk_secrets())

    utils.print_jobs(gatk_client.list_jobs())
