from typing import List
from aztk.utils.command_builder import CommandBuilder
from aztk.utils import helpers
from aztk.utils import constants
from aztk import models as aztk_models
import azure.batch.models as batch_models
POOL_ADMIN_USER_IDENTITY = batch_models.UserIdentity(
    auto_user=batch_models.AutoUserSpecification(
        scope=batch_models.AutoUserScope.pool,
        elevation_level=batch_models.ElevationLevel.admin))

'''
Cluster create helper methods
'''
def __docker_run_cmd(docker_repo: str = None, gpu_enabled: bool = False, file_mounts = []) -> str:
    """
        Build the docker run command by setting up the environment variables
    """

    if gpu_enabled:
        cmd = CommandBuilder('nvidia-docker run')
    else:
        cmd = CommandBuilder('docker run')
    cmd.add_option('--network', 'host')
    cmd.add_option('--name', constants.DOCKER_DASK_CONTAINER_NAME)
    cmd.add_option('-v', '/mnt/batch/tasks:/mnt/batch/tasks')

    if file_mounts:
        for mount in file_mounts:
            cmd.add_option('-v', '{0}:{0}'.format(mount.mount_path))

    cmd.add_option('-e', 'DOCKER_WORKING_DIR=/mnt/batch/tasks/startup/wd')
    cmd.add_option('-e', 'AZ_BATCH_ACCOUNT_NAME=$AZ_BATCH_ACCOUNT_NAME')
    cmd.add_option('-e', 'BATCH_ACCOUNT_KEY=$BATCH_ACCOUNT_KEY')
    cmd.add_option('-e', 'BATCH_ACCOUNT_URL=$BATCH_ACCOUNT_URL')
    cmd.add_option('-e', 'STORAGE_ACCOUNT_NAME=$STORAGE_ACCOUNT_NAME')
    cmd.add_option('-e', 'STORAGE_ACCOUNT_KEY=$STORAGE_ACCOUNT_KEY')
    cmd.add_option('-e', 'STORAGE_ACCOUNT_SUFFIX=$STORAGE_ACCOUNT_SUFFIX')
    cmd.add_option('-e', 'AZ_BATCH_POOL_ID=$AZ_BATCH_POOL_ID')
    cmd.add_option('-e', 'AZ_BATCH_NODE_ID=$AZ_BATCH_NODE_ID')
    cmd.add_option(
        '-e', 'AZ_BATCH_NODE_IS_DEDICATED=$AZ_BATCH_NODE_IS_DEDICATED')
    cmd.add_option('-e', 'DASK_WEB_UI_PORT=$DASK_WEB_UI_PORT')
    cmd.add_option('-p', '8787:8787')       # Dask Web UI
    cmd.add_option('-p', '8788:8788')       # Dask Web UI
    cmd.add_option('-p', '8789:8789')       # Dask Web UI
    cmd.add_option('-p', '8786:8786')       # Dask TCP port
    cmd.add_option('-d', docker_repo)
    cmd.add_argument('/bin/bash /mnt/batch/tasks/startup/wd/docker_main.sh')

    return cmd.to_str()

def __get_docker_credentials(dask_client):
    creds = []

    if dask_client.secrets_config.docker_endpoint:
        creds.append(batch_models.EnvironmentSetting(
            name="DOCKER_ENDPOINT", value=dask_client.secrets_config.docker_endpoint))
    if dask_client.secrets_config.docker_username:
        creds.append(batch_models.EnvironmentSetting(
            name="DOCKER_USERNAME", value=dask_client.secrets_config.docker_username))
    if dask_client.secrets_config.docker_password:
        creds.append(batch_models.EnvironmentSetting(
            name="DOCKER_PASSWORD", value=dask_client.secrets_config.docker_password))

    return creds

def __cluster_install_cmd(zip_resource_file: batch_models.ResourceFile,
                            gpu_enabled: bool,
                            docker_repo: str = None,
                            file_mounts = []):
    """
        For Docker on ubuntu 16.04 - return the command line
        to be run on the start task of the pool to setup dask.
    """
    default_docker_repo = constants.DEFAULT_DOCKER_REPO if not gpu_enabled else constants.DEFAULT_DOCKER_REPO_GPU #TODO: publish dask-gpu image
    docker_repo = docker_repo or default_docker_repo

    shares = []

    if file_mounts:
        for mount in file_mounts:
            # Create the directory on the node
            shares.append('mkdir -p {0}'.format(mount.mount_path))

            # Mount the file share
            shares.append('mount -t cifs //{0}.file.core.windows.net/{2} {3} -o vers=3.0,username={0},password={1},dir_mode=0777,file_mode=0777,sec=ntlmssp'.format(
                mount.storage_account_name,
                mount.storage_account_key,
                mount.file_share_path,
                mount.mount_path
            ))

    setup = [
        'apt-get -y clean',
        'apt-get -y update',
        'apt-get install --fix-missing',
        'apt-get -y install unzip',
        'unzip $AZ_BATCH_TASK_WORKING_DIR/{0}'.format(
            zip_resource_file.file_path),
        'chmod 777 $AZ_BATCH_TASK_WORKING_DIR/setup_node.sh',
        '/bin/bash $AZ_BATCH_TASK_WORKING_DIR/setup_node.sh {0} {1} {2} "{3}"'.format(
            constants.DOCKER_DASK_CONTAINER_NAME,
            gpu_enabled,
            docker_repo,
            __docker_run_cmd(docker_repo, gpu_enabled, file_mounts)),
    ]

    commands = shares + setup
    return commands

def generate_cluster_start_task(
        dask_client,
        zip_resource_file: batch_models.ResourceFile,
        gpu_enabled: bool,
        docker_repo: str = None,
        file_shares: List[aztk_models.FileShare] = None):
    """
        This will return the start task object for the pool to be created.
        :param cluster_id str: Id of the cluster(Used for uploading the resource files)
        :param zip_resource_file: Resource file object pointing to the zip file containing scripts to run on the node
    """

    resource_files = [zip_resource_file]

    dask_web_ui_port = constants.DOCKER_DASK_WEB_UI_PORT

    # TODO use certificate
    environment_settings = [
        batch_models.EnvironmentSetting(
            name="BATCH_ACCOUNT_KEY", value=dask_client.batch_config.account_key),
        batch_models.EnvironmentSetting(
            name="BATCH_ACCOUNT_URL", value=dask_client.batch_config.account_url),
        batch_models.EnvironmentSetting(
            name="STORAGE_ACCOUNT_NAME", value=dask_client.blob_config.account_name),
        batch_models.EnvironmentSetting(
            name="STORAGE_ACCOUNT_KEY", value=dask_client.blob_config.account_key),
        batch_models.EnvironmentSetting(
            name="STORAGE_ACCOUNT_SUFFIX", value=dask_client.blob_config.account_suffix),
        batch_models.EnvironmentSetting(
            name="DASK_WEB_UI_PORT", value=dask_web_ui_port)
    ] + __get_docker_credentials(dask_client)

    # start task command
    command = __cluster_install_cmd(zip_resource_file, gpu_enabled, docker_repo)
    print(helpers.wrap_commands_in_shell(command))

    return batch_models.StartTask(
        command_line=helpers.wrap_commands_in_shell(command),
        resource_files=resource_files,
        environment_settings=environment_settings,
        user_identity=POOL_ADMIN_USER_IDENTITY,
        wait_for_success=True)
