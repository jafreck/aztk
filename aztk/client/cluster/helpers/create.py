import azure.batch.models as batch_models

from aztk import models
from aztk.utils import constants


def create_pool_and_job_and_table(
        core_cluster_operations,
        cluster_conf: models.ClusterConfiguration,
        software_metadata_key: str,
        start_task,
        vm_image_model,
):
    """
        Create a pool and job
        :param cluster_conf: the configuration object used to create the cluster
        :type cluster_conf: aztk.models.ClusterConfiguration
        :parm software_metadata_key: the id of the software being used on the cluster
        :param start_task: the start task for the cluster
        :param VmImageModel: the type of image to provision for the cluster
        :param wait: wait until the cluster is ready
    """
    # save cluster configuration in storage
    core_cluster_operations.get_cluster_data(cluster_conf.cluster_id).save_cluster_config(cluster_conf)

    core_cluster_operations.create_batch_resources(
        id=cluster_conf.cluster_id,
        start_task=start_task,
        job_manager_task=None,
        vm_size=cluster_conf.vm_size,
        vm_image_model=vm_image_model,
        on_all_tasks_complete=batch_models.OnAllTasksComplete.no_action,
        mixed_mode=cluster_conf.mixed_mode,
        software_metadata_key=software_metadata_key,
        mode_metadata_key=constants.AZTK_CLUSTER_MODE_METADATA,
        size_dedicated=cluster_conf.size,
        size_low_priority=cluster_conf.size_low_priority,
        subnet_id=cluster_conf.subnet_id,
        job_metadata=[],
    )

    # create storage task table
    if cluster_conf.scheduling_target != models.SchedulingTarget.Any:
        core_cluster_operations.create_task_table(cluster_conf.cluster_id)

    return core_cluster_operations.get(cluster_conf.cluster_id)
