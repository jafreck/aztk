from typing import List
import os
import yaml
import datetime
import azure.batch.models as batch_models
from aztk.utils import constants, helpers
from aztk.utils.command_builder import CommandBuilder

'''
Submit helper methods
'''


def __get_node(spark_client, node_id: str, cluster_id: str) -> batch_models.ComputeNode:
    return spark_client.batch_client.compute_node.get(cluster_id, node_id)


def generate_task(spark_client, container_id, application):
    resource_files = []

    app_resource_file = helpers.upload_file_to_container(container_name=container_id,
                                                         application_name=application.name,
                                                         file_path=application.application,
                                                         blob_client=spark_client.blob_client,
                                                         use_full_path=False)

    # Upload application file
    resource_files.append(app_resource_file)

    # Upload dependent JARS
    jar_resource_file_paths = []
    for jar in application.jars:
        current_jar_resource_file_path = helpers.upload_file_to_container(container_name=container_id,
                                                                          application_name=application.name,
                                                                          file_path=jar,
                                                                          blob_client=spark_client.blob_client,
                                                                          use_full_path=False)
        jar_resource_file_paths.append(current_jar_resource_file_path)
        resource_files.append(current_jar_resource_file_path)

    # Upload dependent python files
    py_files_resource_file_paths = []
    for py_file in application.py_files:
        current_py_files_resource_file_path = helpers.upload_file_to_container(container_name=container_id,
                                                                               application_name=application.name,
                                                                               file_path=py_file,
                                                                               blob_client=spark_client.blob_client,
                                                                               use_full_path=False)
        py_files_resource_file_paths.append(
            current_py_files_resource_file_path)
        resource_files.append(current_py_files_resource_file_path)

    # Upload other dependent files
    files_resource_file_paths = []
    for file in application.files:
        files_resource_file_path = helpers.upload_file_to_container(container_name=container_id,
                                                                    application_name=application.name,
                                                                    file_path=file,
                                                                    blob_client=spark_client.blob_client,
                                                                    use_full_path=False)
        print(files_resource_file_path.file_path)
        files_resource_file_paths.append(files_resource_file_path)
        resource_files.append(files_resource_file_path)

    # Upload application definition
    application.application = os.path.basename(application.application)
    application.jars = [os.path.basename(jar) for jar in application.jars]
    application.py_files = [os.path.basename(py_files) for py_files in application.py_files]
    application.files = [os.path.basename(files) for files in application.files]
    application_definition_file = helpers.upload_text_to_container(
        container_name=container_id,
        application_name=application.name,
        file_path='application.yaml',
        content=yaml.dump(vars(application)),
        blob_client=spark_client.blob_client)
    resource_files.append(application_definition_file)

    # create command to submit task
    # TODO: convert to CommandBuliders
    task_cmd = 'sudo docker exec -i -e AZ_BATCH_TASK_WORKING_DIR=$AZ_BATCH_TASK_WORKING_DIR -e STORAGE_LOGS_CONTAINER={0} spark /bin/bash >> output.log 2>&1 -c "cd $AZ_BATCH_TASK_WORKING_DIR; python \$DOCKER_WORKING_DIR/submit.py"'.format(container_id)

    # Create task
    task = batch_models.TaskAddParameter(
        id=application.name,
        command_line=helpers.wrap_commands_in_shell([task_cmd]),
        resource_files=resource_files,
        user_identity=batch_models.UserIdentity(
            auto_user=batch_models.AutoUserSpecification(
                scope=batch_models.AutoUserScope.task,
                elevation_level=batch_models.ElevationLevel.admin))
    )

    return task


def submit_application(spark_client, cluster_id, application, wait: bool = False):
    """
    Submit a spark app
    """
    # cluster = spark_client.get_cluster(cluster_id)
    # application.gpu_enabled = cluster.gpu_enabled
    task = generate_task(spark_client, cluster_id, application)

    # Add task to batch job (which has the same name as cluster_id)
    job_id = cluster_id
    spark_client.batch_client.task.add(job_id=job_id, task=task)

    if wait:
        helpers.wait_for_task_to_complete(job_id=job_id, task_id=task.id, batch_client=spark_client.batch_client)
