from datetime import timedelta

import azure.batch.models as batch_models

from aztk.utils import constants, helpers


def create_batch_resources(
        batch_client,
        id,
        start_task,
        job_manager_task,
        vm_size,
        vm_image_model,
        on_all_tasks_complete,
        mixed_mode,
        software_metadata_key,
        size_dedicated,
        size_low_priority,
        subnet_id,
):
    autoscale_formula = "$TargetDedicatedNodes = {0}; " "$TargetLowPriorityNodes = {1}".format(
        size_dedicated, size_low_priority)

    sku_to_use, image_ref_to_use = helpers.select_latest_verified_vm_image_with_node_agent_sku(
        vm_image_model.publisher, vm_image_model.offer, vm_image_model.sku, batch_client)

    network_conf = None
    if subnet_id is not None:
        network_conf = batch_models.NetworkConfiguration(subnet_id=subnet_id)

    auto_pool_specification = batch_models.AutoPoolSpecification(
        pool_lifetime_option=batch_models.PoolLifetimeOption.job,
        auto_pool_id_prefix=id,
        keep_alive=False,
        pool=batch_models.PoolSpecification(
            display_name=id,
            virtual_machine_configuration=batch_models.VirtualMachineConfiguration(
                image_reference=image_ref_to_use, node_agent_sku_id=sku_to_use),
            vm_size=vm_size,
            enable_auto_scale=True,
            auto_scale_formula=autoscale_formula,
            auto_scale_evaluation_interval=timedelta(minutes=5),
            start_task=start_task,
            enable_inter_node_communication=not mixed_mode,
            network_configuration=network_conf,
            max_tasks_per_node=4,
            metadata=[
                batch_models.MetadataItem(name=constants.AZTK_SOFTWARE_METADATA_KEY, value=software_metadata_key),
                batch_models.MetadataItem(
                    name=constants.AZTK_MODE_METADATA_KEY, value=constants.AZTK_JOB_MODE_METADATA),
            ],
        ),
    )

    job = batch_models.JobAddParameter(
        id=id,
        pool_info=batch_models.PoolInformation(auto_pool_specification=auto_pool_specification),
        job_manager_task=job_manager_task,
        on_all_tasks_complete=on_all_tasks_complete,
    )

    return batch_client.job.add(job)
