import argparse
import typing
from cli import log
from cli.dask.aztklib import load_dask_client


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='cluster_id',
                        required=True,
                        help='The unique id of your dask cluster')
    parser.add_argument('--force',
                        dest='force',
                        required=False,
                        action='store_true',
                        help='Do not prompt for confirmation, force deletion of cluster.')
    parser.set_defaults(force=False)

    
def execute(args: typing.NamedTuple):
    dask_client = load_dask_client()
    cluster_id = args.cluster_id

    if not args.force:
        confirmation_cluster_id = input("Please confirm the id of the cluster you wish to delete: ")

        if confirmation_cluster_id  != cluster_id:
            log.error("Confirmation cluster id does not match. Please try again.")
            return

    if dask_client.delete_cluster(cluster_id):
        log.info("Deleting cluster %s", cluster_id)
    else:
        log.error("Cluster with id '%s' doesn't exist or was already deleted.", cluster_id)
