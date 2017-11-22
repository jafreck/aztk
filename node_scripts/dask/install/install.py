import os
from core import config
from install import pick_master, dask, scripts

def setup_node():
    client = config.batch_client

    dask.setup_conf()

    is_master = pick_master.find_master(client)

    if is_master:
        setup_as_master()
        scripts.run_custom_scripts(is_master = True)

    else:
        setup_as_worker()
        scripts.run_custom_scripts(is_master = False)


def setup_as_master():
    print("Setting up as master.")
    dask.start_dask_master()
    dask.start_dask_worker()


def setup_as_worker():
    print("Setting up as worker.")
    dask.start_dask_worker()
