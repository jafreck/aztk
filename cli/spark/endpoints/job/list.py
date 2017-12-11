import argparse
import typing
import time
from cli.spark.aztklib import load_spark_client
from cli import utils

def setup_parser(_: argparse.ArgumentParser):
    # No arguments for list yet
    pass

def execute(args: typing.NamedTuple):
    spark_client = load_spark_client()

    print(spark_client.list_jobs())
