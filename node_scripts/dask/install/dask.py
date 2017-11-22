"""
    Code that handle dask configuration
"""
import datetime
import time
import os
import json
import shutil
from subprocess import call, Popen, check_output
from typing import List
import azure.batch.models as batchmodels
from core import config
from install import pick_master
from distributed import Scheduler
from tornado.ioloop import IOLoop
from threading import Thread

batch_client = config.batch_client

def get_pool() -> batchmodels.CloudPool:
    return batch_client.pool.get(config.pool_id)


def get_node(node_id: str) -> batchmodels.ComputeNode:
    return batch_client.compute_node.get(config.pool_id, node_id)


def list_nodes() -> List[batchmodels.ComputeNode]:
    """
        List all the nodes in the pool.
    """
    # TODO use continuation token & verify against current/target dedicated of
    # pool
    return batch_client.compute_node.list(config.pool_id)


def wait_for_master():
    print("Waiting for master to be ready.")
    master_node_id = pick_master.get_master_node_id(
        batch_client.pool.get(config.pool_id))

    if master_node_id == config.node_id:
        return

    while True:
        master_node = get_node(master_node_id)

        if master_node.state in [batchmodels.ComputeNodeState.idle, batchmodels.ComputeNodeState.running]:
            break
        else:
            print("{0} Still waiting on master", datetime.datetime.now())
            time.sleep(10)


def start_dask_task_scheduler():
    from distributed import Scheduler

    loop = IOLoop.current()
    t = Thread(target=loop.start, daemon=True)
    t.start()

    s = Scheduler(loop=loop)
    s.start('tcp://:8786')   # Listen on TCP port 8786



def start_dask_worker():
    from distributed import Nanny

    loop = IOLoop.current()
    t = Thread(target=loop.start, daemon=True)
    t.start()

    w = Nanny('tcp://127.0.0.1:8786', loop=loop)
    w.start()  # choose randomly assigned port


def setup_conf():
    """
        Set up dask configuration
    """
    pass