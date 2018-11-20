from datetime import datetime


def get_test_suffix(prefix: str):
    # base cluster name
    dt = datetime.now()
    current_time = dt.microsecond
    base_cluster_id = "{0}-{1}".format(prefix, current_time)
    return base_cluster_id
