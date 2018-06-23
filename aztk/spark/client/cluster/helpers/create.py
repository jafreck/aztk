from typing import List

import azure.batch.models as batch_models
import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.internal.cluster_data import NodeData
from aztk.spark import models
from aztk.spark.utils import util
from aztk.utils import constants, helpers

POOL_ADMIN_USER_IDENTITY = batch_models.UserIdentity(
    auto_user=batch_models.AutoUserSpecification(
        scope=batch_models.AutoUserScope.pool, elevation_level=batch_models.ElevationLevel.admin))

def _default_scheduling_target(vm_count: int):
    if vm_count == 0:
        return models.SchedulingTarget.Any
    else:
        return models.SchedulingTarget.Dedicated


def _apply_default_for_cluster_config(configuration: models.ClusterConfiguration):
    cluster_conf = models.ClusterConfiguration()
    cluster_conf.merge(configuration)
    if cluster_conf.scheduling_target is None:
        cluster_conf.scheduling_target = _default_scheduling_target(cluster_conf.size)
    return cluster_conf


def _get_aztk_environment(cluster_id, worker_on_master, mixed_mode):
    envs = []
    envs.append(batch_models.EnvironmentSetting(name="AZTK_MIXED_MODE", value=helpers.bool_env(mixed_mode)))
    envs.append(batch_models.EnvironmentSetting(name="AZTK_WORKER_ON_MASTER", value=helpers.bool_env(worker_on_master)))
    envs.append(batch_models.EnvironmentSetting(name="AZTK_CLUSTER_ID", value=cluster_id))
    return envs


def __get_docker_credentials(spark_client):
    creds = []
    docker = spark_client.secrets_config.docker
    if docker:
        if docker.endpoint:
            creds.append(batch_models.EnvironmentSetting(name="DOCKER_ENDPOINT", value=docker.endpoint))
        if docker.username:
            creds.append(batch_models.EnvironmentSetting(name="DOCKER_USERNAME", value=docker.username))
        if docker.password:
            creds.append(batch_models.EnvironmentSetting(name="DOCKER_PASSWORD", value=docker.password))

    return creds


def __get_secrets_env(spark_client):
    shared_key = spark_client.secrets_config.shared_key
    service_principal = spark_client.secrets_config.service_principal
    if shared_key:
        return [
            batch_models.EnvironmentSetting(name="BATCH_SERVICE_URL", value=shared_key.batch_service_url),
            batch_models.EnvironmentSetting(name="BATCH_ACCOUNT_KEY", value=shared_key.batch_account_key),
            batch_models.EnvironmentSetting(name="STORAGE_ACCOUNT_NAME", value=shared_key.storage_account_name),
            batch_models.EnvironmentSetting(name="STORAGE_ACCOUNT_KEY", value=shared_key.storage_account_key),
            batch_models.EnvironmentSetting(name="STORAGE_ACCOUNT_SUFFIX", value=shared_key.storage_account_suffix),
        ]
    else:
        return [
            batch_models.EnvironmentSetting(name="SP_TENANT_ID", value=service_principal.tenant_id),
            batch_models.EnvironmentSetting(name="SP_CLIENT_ID", value=service_principal.client_id),
            batch_models.EnvironmentSetting(name="SP_CREDENTIAL", value=service_principal.credential),
            batch_models.EnvironmentSetting(
                name="SP_BATCH_RESOURCE_ID", value=service_principal.batch_account_resource_id),
            batch_models.EnvironmentSetting(
                name="SP_STORAGE_RESOURCE_ID", value=service_principal.storage_account_resource_id),
        ]


def __cluster_install_cmd(zip_resource_file: batch_models.ResourceFile,
                          gpu_enabled: bool,
                          docker_repo: str = None,
                          plugins=None,
                          worker_on_master: bool = True,
                          file_mounts=None,
                          mixed_mode: bool = False):
    """
        For Docker on ubuntu 16.04 - return the command line
        to be run on the start task of the pool to setup spark.
    """
    default_docker_repo = constants.DEFAULT_DOCKER_REPO if not gpu_enabled else constants.DEFAULT_DOCKER_REPO_GPU
    docker_repo = docker_repo or default_docker_repo

    shares = []

    if file_mounts:
        for mount in file_mounts:
            # Create the directory on the node
            shares.append('mkdir -p {0}'.format(mount.mount_path))

            # Mount the file share
            shares.append(
                'mount -t cifs //{0}.file.core.windows.net/{2} {3} -o vers=3.0,username={0},password={1},dir_mode=0777,file_mode=0777,sec=ntlmssp'.
                format(mount.storage_account_name, mount.storage_account_key, mount.file_share_path, mount.mount_path))

    setup = [
        'time('\
            'apt-get -y update;'\
            'apt-get -y --no-install-recommends install unzip;'\
            'unzip -o $AZ_BATCH_TASK_WORKING_DIR/{0};'\
            'chmod 777 $AZ_BATCH_TASK_WORKING_DIR/aztk/node_scripts/setup_host.sh;'\
        ') 2>&1'.format(zip_resource_file.file_path),
        '/bin/bash $AZ_BATCH_TASK_WORKING_DIR/aztk/node_scripts/setup_host.sh {0} {1}'.format(
            constants.DOCKER_SPARK_CONTAINER_NAME,
            docker_repo,
        )
    ]

    commands = shares + setup
    return commands


def generate_cluster_start_task(spark_client,
                                zip_resource_file: batch_models.ResourceFile,
                                cluster_id: str,
                                gpu_enabled: bool,
                                docker_repo: str = None,
                                file_shares: List[models.FileShare] = None,
                                plugins: List[models.PluginConfiguration] = None,
                                mixed_mode: bool = False,
                                worker_on_master: bool = True):
    """
        This will return the start task object for the pool to be created.
        :param cluster_id str: Id of the cluster(Used for uploading the resource files)
        :param zip_resource_file: Resource file object pointing to the zip file containing scripts to run on the node
    """

    resource_files = [zip_resource_file]
    spark_web_ui_port = constants.DOCKER_SPARK_WEB_UI_PORT
    spark_worker_ui_port = constants.DOCKER_SPARK_WORKER_UI_PORT
    spark_job_ui_port = constants.DOCKER_SPARK_JOB_UI_PORT

    spark_container_name = constants.DOCKER_SPARK_CONTAINER_NAME
    spark_submit_logs_file = constants.SPARK_SUBMIT_LOGS_FILE

    # TODO use certificate
    environment_settings = __get_secrets_env(spark_client) + [
        batch_models.EnvironmentSetting(name="SPARK_WEB_UI_PORT", value=spark_web_ui_port),
        batch_models.EnvironmentSetting(name="SPARK_WORKER_UI_PORT", value=spark_worker_ui_port),
        batch_models.EnvironmentSetting(name="SPARK_JOB_UI_PORT", value=spark_job_ui_port),
        batch_models.EnvironmentSetting(name="SPARK_CONTAINER_NAME", value=spark_container_name),
        batch_models.EnvironmentSetting(name="SPARK_SUBMIT_LOGS_FILE", value=spark_submit_logs_file),
        batch_models.EnvironmentSetting(name="AZTK_GPU_ENABLED", value=helpers.bool_env(gpu_enabled)),
    ] + __get_docker_credentials(spark_client) + _get_aztk_environment(cluster_id, worker_on_master, mixed_mode)

    # start task command
    command = __cluster_install_cmd(zip_resource_file, gpu_enabled, docker_repo, plugins, worker_on_master, file_shares,
                                    mixed_mode)

    return batch_models.StartTask(
        command_line=helpers.wrap_commands_in_shell(command),
        resource_files=resource_files,
        environment_settings=environment_settings,
        user_identity=POOL_ADMIN_USER_IDENTITY,
        wait_for_success=True)


def create_cluster(spark_cluster_client, cluster_conf: models.ClusterConfiguration, wait: bool = False):
    """
    Create a new aztk spark cluster

    Args:
        cluster_conf(aztk.spark.models.models.ClusterConfiguration): Configuration for the the cluster to be created
        wait(bool): If you should wait for the cluster to be ready before returning

    Returns:
        aztk.spark.models.Cluster
    """
    cluster_conf = _apply_default_for_cluster_config(cluster_conf)
    cluster_conf.validate()

    cluster_data = spark_cluster_client._get_cluster_data(cluster_conf.cluster_id)
    try:
        zip_resource_files = None
        node_data = NodeData(cluster_conf).add_core().done()
        zip_resource_files = cluster_data.upload_node_data(node_data).to_resource_file()

        start_task = generate_cluster_start_task(spark_cluster_client, zip_resource_files, cluster_conf.cluster_id,
                                                 cluster_conf.gpu_enabled(), cluster_conf.get_docker_repo(),
                                                 cluster_conf.file_shares, cluster_conf.plugins,
                                                 cluster_conf.mixed_mode(), cluster_conf.worker_on_master)

        software_metadata_key = "spark"

        vm_image = models.VmImage(publisher='Canonical', offer='UbuntuServer', sku='16.04')

        cluster = spark_cluster_client.create_pool_and_job(cluster_conf, software_metadata_key, start_task, vm_image)

        # Wait for the master to be ready
        if wait:
            util.wait_for_master_to_be_ready(spark_cluster_client, cluster.id)
            cluster = spark_cluster_client.get_cluster(cluster.id)

        return cluster

    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
