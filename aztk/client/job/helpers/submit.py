from datetime import timedelta

import azure.batch.models as batch_models

from aztk import models
from aztk.utils import constants, helpers


def submit_job(
        core_job_operations,
        job_configuration,
        start_task,
        job_manager_task,
        autoscale_formula,
        software_metadata_key: str,
        vm_image_model,
        application_metadata,
):
    """
            Job Submission
            :param job_configuration -> aztk_sdk.spark.models.JobConfiguration
            :param start_task -> batch_models.StartTask
            :param job_manager_task -> batch_models.TaskAddParameter
            :param autoscale_formula -> str
            :param software_metadata_key -> str
            :param vm_image_model -> aztk_sdk.models.VmImage
            :returns None
        """
    core_job_operations.get_cluster_data(job_configuration.id).save_cluster_config(
        job_configuration.to_cluster_config())

    core_job_operations.create_batch_resources(
        id=job_configuration.cluster_id,
        start_task=start_task,
        job_manager_task=job_manager_task,
        vm_size=job_configuration.vm_size,
        vm_image_model=vm_image_model,
        on_all_tasks_complete=batch_models.OnAllTasksComplete.terminate_job,
        mixed_mode=job_configuration.mixed_mode,
        software_metadata_key=software_metadata_key,
        size_dedicated=job_configuration.max_dedicated_nodes,
        size_low_priority=job_configuration.max_low_pri_nodes,
        subnet_id=job_configuration.subnet_id,
    )

    if job_configuration.scheduling_target != models.SchedulingTarget.Any:
        core_job_operations.create_task_table(job_configuration.id)

    return core_job_operations.batch_client.job_schedule.get(job_schedule_id=job_configuration.id)
