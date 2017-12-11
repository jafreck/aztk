import argparse
import typing
from cli import log
from cli.spark.aztklib import load_spark_client


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your AZTK Job')
    parser.add_argument('--force',
                        dest='force',
                        required=False,
                        action='store_true',
                        help='Do not prompt for confirmation, force deletion of cluster.')
    parser.set_defaults(force=False)

    
def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()
    job_id = args.job_id

    if not args.force:
        confirmation_cluster_id = input("Please confirm the id of the cluster you wish to delete: ")

        if confirmation_cluster_id  != job_id:
            log.error("Confirmation cluster id does not match. Please try again.")
            return

    if spark_client.delete_job(job_id):
        log.info("Deleting cluster %s", job_id)
    else:
        log.error("Cluster with id '%s' doesn't exist or was already deleted.", job_id)
