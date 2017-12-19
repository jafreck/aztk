import argparse
import typing
from cli.dask.aztklib import load_dask_client
from cli import utils


def setup_parser(_: argparse.ArgumentParser):
    # No arguments for list yet
    pass


def execute(_: typing.NamedTuple):
    dask_client = load_dask_client()
    clusters = dask_client.list_clusters()
    utils.print_clusters(clusters)
