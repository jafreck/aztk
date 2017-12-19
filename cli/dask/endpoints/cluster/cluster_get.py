import argparse
import typing
from cli.dask.aztklib import load_dask_client
from cli import utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your dask cluster')


def execute(args: typing.NamedTuple):
    dask_client = load_dask_client()
    cluster_id = args.cluster_id
    cluster = dask_client.get_cluster(cluster_id)
    utils.print_cluster(dask_client, cluster)
