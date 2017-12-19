from typing import List

import azure.batch.models as batch_models
from aztk.utils import constants, helpers
from aztk.utils.command_builder import CommandBuilder

'''
Submit helper methods
'''


def __get_node(dask_client, node_id: str, cluster_id: str) -> batch_models.ComputeNode:
    return dask_client.batch_client.compute_node.get(cluster_id, node_id)


def __app_submit_cmd(
        dask_client,
        cluster_id: str,
        name: str,
        app: str,
        app_args: [],
        files: []):
    cluster = dask_client.get_cluster(cluster_id)
    master_id = cluster.master_node_id
    master_ip = __get_node(dask_client, master_id, cluster_id).ip_address


    # set file paths to correct path on container
    files_path = 'mnt/batch/tasks/workitems/{0}/{1}/{2}/wd/'.format(cluster_id, "job-1", name)
    files = [files_path + f for f in files]

    # 2>&1 redirect stdout and stderr to be in the same file
    # dask_submit_cmd = CommandBuilder('dask-submit')
    # dask_submit_cmd.add_option('{0}:7077'.format(master_ip), enable=True)
    # dask_submit_cmd.add_argument(app)

    # app_args = ' '.join(['\'' + app_arg + '\'' for app_arg in (app_args if app_args else [])])
    # dask_submit_cmd.add_argument(
    #     'mnt/batch/tasks/workitems/{0}/{1}/{2}/wd/'.format(cluster_id, "job-1", name) +
    #     app + ' ' + app_args)
    
    app_args = ' '.join(['\'' + app_arg + '\'' for app_arg in (app_args if app_args else [])])
    dask_submit_cmd = CommandBuilder('python')
    dask_submit_cmd.add_argument(app + ' ' + app_args)

    docker_exec_cmd = CommandBuilder('sudo docker exec')
    docker_exec_cmd.add_option('-i', constants.DOCKER_DASK_CONTAINER_NAME)
    docker_exec_cmd.add_argument('/bin/bash  >> {0} 2>&1 -c \"cd '.format(
        constants.SPARK_SUBMIT_LOGS_FILE) + files_path + '; ' + dask_submit_cmd.to_str() + '\"')

    print(docker_exec_cmd.to_str())

    return [
        docker_exec_cmd.to_str()
    ]


def submit_application(dask_client, cluster_id, application, wait: bool = False):
    """
    Submit a dask app
    """

    resource_files = []

    app_resource_file = helpers.upload_file_to_container(container_name=application.name,
                                                         file_path=application.application,
                                                         blob_client=dask_client.blob_client,
                                                         use_full_path=False)

    # Upload application file
    resource_files.append(app_resource_file)

    # Upload other dependent files
    files_resource_file_paths = []
    for file in application.files:
        files_resource_file_path = helpers.upload_file_to_container(container_name=application.name,
                                                                    file_path=file,
                                                                    blob_client=dask_client.blob_client,
                                                                    use_full_path=False)
        files_resource_file_paths.append(files_resource_file_path)
        resource_files.append(files_resource_file_path)

    # create command to submit task
    cmd = __app_submit_cmd(
        dask_client=dask_client,
        cluster_id=cluster_id,
        name=application.name,
        app=app_resource_file.file_path,
        app_args=application.application_args,
        files=[file_resource_file_path.file_path for file_resource_file_path in files_resource_file_paths]
    )

    # Get cluster size
    cluster = dask_client.get_cluster(cluster_id)

    # Affinitize task to master node
    # master_node_affinity_id = helpers.get_master_node_id(cluster_id, dask_client.batch_client)
    rls = dask_client.get_remote_login_settings(cluster.id, cluster.master_node_id)

    # Create task
    task = batch_models.TaskAddParameter(
        id=application.name,
        affinity_info=batch_models.AffinityInformation(
            affinity_id=cluster.master_node_id),
        command_line=helpers.wrap_commands_in_shell(cmd),
        resource_files=resource_files,
        user_identity=batch_models.UserIdentity(
            auto_user=batch_models.AutoUserSpecification(
                scope=batch_models.AutoUserScope.task,
                elevation_level=batch_models.ElevationLevel.admin))
    )

    # Add task to batch job (which has the same name as cluster_id)
    job_id = cluster_id
    dask_client.batch_client.task.add(job_id=job_id, task=task)

    if wait:
        helpers.wait_for_task_to_complete(job_id=job_id, task_id=task.id, batch_client=dask_client.batch_client)
