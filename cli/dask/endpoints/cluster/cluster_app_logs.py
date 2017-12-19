import argparse
import typing
import time
from cli.dask.aztklib import load_dask_client
from cli import utils

def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your dask cluster')
    parser.add_argument('--name',
                        dest='app_name',
                        required=True,
                        help='The unique id of your job name')

    parser.add_argument('--tail', dest='tail', action='store_true')


def execute(args: typing.NamedTuple):
    dask_client = load_dask_client()

    if args.tail:
        utils.stream_logs(client=dask_client, cluster_id=args.cluster_id, application_name=args.app_name)
    else:
        app_logs = dask_client.get_application_log(cluster_id=args.cluster_id, application_name=args.app_name)
        print(app_logs.log)
