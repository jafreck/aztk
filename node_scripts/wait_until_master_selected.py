import time

master = None

while master is None:
    try:
        from core import config
        from install.pick_master import get_master_node_id

        batch_client = config.batch_client
        pool = batch_client.pool.get(config.pool_id)
        master = get_master_node_id(pool)

    except Exception as e:
        time.sleep(1)
