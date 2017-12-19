import argparse
import typing
from cli import log
from cli import utils
from cli.dask.aztklib import load_dask_client
import aztk.dask


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id', dest='cluster_id', required=True,
                        help='The unique id of your dask cluster')

    parser.add_argument('--name', required=True,
                        help='a name for your application')

    parser.add_argument('--wait', dest='wait', action='store_true',
                        help='Wait for app to complete')
    parser.add_argument('--no-wait', dest='wait', action='store_false',
                        help='Do not wait for app to complete')
    parser.set_defaults(wait=True)
    parser.add_argument('app',
                        help='App jar OR python file to execute. Use absolute \
                              path to reference file.')

    parser.add_argument('app_args', nargs='*',
                        help='Arguments for the application')


def execute(args: typing.NamedTuple):
    dask_client = load_dask_client()

    log.info("-------------------------------------------")
    log.info("dask cluster id:        %s", args.cluster_id)
    log.info("dask app name:          %s", args.name)
    log.info("Wait for app completion: %s", args.wait)
    log.info("Application:             %s", args.app)
    log.info("Application arguments:   %s", args.app_args)
    log.info("-------------------------------------------")


    dask_client.submit(
        cluster_id=args.cluster_id,
        application=aztk.dask.models.Application(
            name=args.name,
            application=args.app,
            application_args=args.app_args
        ),
        wait=False
    )

    if args.wait:
        utils.stream_logs(client=dask_client, cluster_id=args.cluster_id, application_name=args.name)
