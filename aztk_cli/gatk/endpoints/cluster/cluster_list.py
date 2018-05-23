import argparse
import typing

import aztk
from aztk_cli import config, utils


def setup_parser(_: argparse.ArgumentParser):
    # No arguments for list yet
    pass


def execute(_: typing.NamedTuple):
    gatk_client = aztk.gatk.Client(config.load_aztk_secrets())
    clusters = gatk_client.list_clusters()
    utils.print_clusters(clusters)
