import os
from core import config
from install import pick_master, spark, scripts
import wait_until_master_selected

def setup_node():
    client = config.batch_client

    spark.setup_conf()

    if os.environ['AZ_BATCH_NODE_IS_DEDICATED'] == "true" or os.environ['MIXED_MODE'] == "False":
        is_master = pick_master.find_master(client)
    else:
        is_master = False
        wait_until_master_selected.main()

    master_node_id = pick_master.get_master_node_id(config.batch_client.pool.get(config.pool_id))
    master_node = config.batch_client.compute_node.get(config.pool_id, master_node_id)

    os.environ["MASTER_IP"] = master_node.ip_address

    if is_master:
        setup_as_master()
        scripts.run_custom_scripts(is_master = True)

    else:
        setup_as_worker()
        scripts.run_custom_scripts(is_master = False)


def setup_as_master():
    print("Setting up as master.")
    spark.setup_connection()
    spark.start_spark_master()
    spark.start_spark_worker()


def setup_as_worker():
    print("Setting up as worker.")
    spark.setup_connection()
    spark.start_spark_worker()
