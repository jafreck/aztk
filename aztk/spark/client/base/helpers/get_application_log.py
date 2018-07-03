from aztk.spark import models


def get_application_log(base_operations, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
    base_application_log = super(type(base_operations), base_operations).cluster.get_application_log(
        cluster_id, application_name, tail, current_bytes)
    return models.ApplicationLog(base_application_log.name, base_application_log.cluster_id, base_application_log.log,
                                 base_application_log.total_bytes, base_application_log.application_state,
                                 base_application_log.exit_code)
