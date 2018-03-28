import os
import argparse
import typing
import aztk.spark
from aztk_cli import utils, config


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id',
                        dest='job_id',
                        required=True,
                        help='The unique id of your AZTK job')
    parser.add_argument('--name',
                        dest='app_name',
                        required=True,
                        help='The unique id of your job name')
    parser.add_argument('--output',
                        help='Path to the file you wish to output to. If not \
                              specified, output is printed to stdout')


def execute(args: typing.NamedTuple):
    spark_client = aztk.spark.Client(config.load_aztk_secrets())
    app_log = spark_client.get_job_application_log(args.job_id, args.app_name)
    if args.output:
        spinner = utils.Spinner()
        spinner.start()
        with open(os.path.abspath(os.path.expanduser(args.output)), "w", encoding="UTF-8") as f:
            f.write(app_log.log)
        spinner.stop()
    else:
        print(app_log.log)
