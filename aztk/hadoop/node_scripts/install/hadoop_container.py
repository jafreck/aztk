import subprocess

from aztk.internal import DockerCmd
from aztk.utils import constants

def start_hadoop_container(
        docker_repo: str=None,
        gpu_enabled: bool=False,
        file_mounts=None,
        plugins=None,
        is_master: bool = False,
        master_ip: str = None):

    docker_run_cmd="yarn-resourcemanager" if is_master else "yarn-nodemanager {}".format(master_ip)
    cmd = DockerCmd(
        name=constants.DOCKER_HADOOP_CONTAINER_NAME,
        docker_repo=docker_repo,
        cmd=docker_run_cmd, # use default entrypoint
        gpu_enabled=gpu_enabled)

    if file_mounts:
        for mount in file_mounts:
            cmd.share_folder(mount.mount_path)
    cmd.share_folder('/mnt')

    cmd.pass_env('AZTK_WORKING_DIR')
    cmd.pass_env('AZ_BATCH_ACCOUNT_NAME')
    cmd.pass_env('BATCH_ACCOUNT_KEY')
    cmd.pass_env('BATCH_SERVICE_URL')
    cmd.pass_env('STORAGE_ACCOUNT_NAME')
    cmd.pass_env('STORAGE_ACCOUNT_KEY')
    cmd.pass_env('STORAGE_ACCOUNT_SUFFIX')

    cmd.pass_env('SP_TENANT_ID')
    cmd.pass_env('SP_CLIENT_ID')
    cmd.pass_env('SP_CREDENTIAL')
    cmd.pass_env('SP_BATCH_RESOURCE_ID')
    cmd.pass_env('SP_STORAGE_RESOURCE_ID')

    cmd.pass_env('AZ_BATCH_POOL_ID')
    cmd.pass_env('AZ_BATCH_NODE_ID')
    cmd.pass_env('AZ_BATCH_NODE_IS_DEDICATED')

    cmd.pass_env('AZTK_WORKER_ON_MASTER')
    cmd.pass_env('AZTK_MIXED_MODE')
    cmd.pass_env('AZTK_IS_MASTER')
    cmd.pass_env('AZTK_IS_WORKER')
    cmd.pass_env('AZTK_MASTER_IP')

    cmd.pass_env('DOCKER_HADOOP_WEB_UI_PORT')
    # cmd.pass_env('hadoop_WORKER_UI_PORT')
    # cmd.pass_env('hadoop_CONTAINER_NAME')
    # cmd.pass_env('HADOOP_SUBMIT_LOGS_FILE')
    # cmd.pass_env('hadoop_JOB_UI_PORT')

    cmd.open_port(8088)       # Hadoop Web UI
    # cmd.open_port(7077)       # hadoop Master
    # cmd.open_port(7337)       # hadoop Shuffle Service
    # cmd.open_port(4040)       # Job UI
    # cmd.open_port(18080)      # hadoop History Server UI
    # cmd.open_port(3022)       # Docker SSH

    if plugins:
        for plugin in plugins:
            for port in plugin.ports:
                cmd.open_port(port.internal)

    print("="*60)
    print("                 Starting docker container")
    print("-"*60)
    print(cmd.to_str())
    print("="*60)
    subprocess.call(['/bin/bash', '-c', 'echo Is master?: $AZTK_IS_MASTER _ $AZTK_IS_WORKER'])
    subprocess.call(['/bin/bash', '-c', cmd.to_str()])
