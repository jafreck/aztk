"""
    Code that handle hadoop configuration
"""
import datetime
import time
import os
import json
import shutil
from subprocess import call, Popen, check_output
from typing import List
import azure.batch.models as batchmodels
from core import config
from install import pick_master

batch_client = config.batch_client

hadoop_home = "/home/hadoop-current"
hadoop_conf_folder = os.path.join(hadoop_home, "conf")


def setup_as_master():
    print("Setting up as master.")
    setup_connection()
    start_hadoop_master()


def setup_as_worker():
    print("Setting up as worker.")
    setup_connection()
    start_hadoop_worker()

def get_pool() -> batchmodels.CloudPool:
    return batch_client.pool.get(config.pool_id)


def get_node(node_id: str) -> batchmodels.ComputeNode:
    return batch_client.compute_node.get(config.pool_id, node_id)


def list_nodes() -> List[batchmodels.ComputeNode]:
    """
        List all the nodes in the pool.
    """
    # TODO use continuation token & verify against current/target dedicated of
    # pool
    return batch_client.compute_node.list(config.pool_id)


def setup_connection():
    """
        This setup hadoop config with which nodes are slaves and which are master
    """
    master_node_id = pick_master.get_master_node_id(
        batch_client.pool.get(config.pool_id))
    master_node = get_node(master_node_id)

    master_config_file = os.path.join(hadoop_conf_folder, "master")
    master_file = open(master_config_file, 'w', encoding='UTF-8')

    print("Adding master node ip {0} to config file '{1}'".format(
        master_node.ip_address, master_config_file))
    master_file.write("{0}\n".format(master_node.ip_address))

    master_file.close()


def wait_for_master():
    print("Waiting for master to be ready.")
    master_node_id = pick_master.get_master_node_id(
        batch_client.pool.get(config.pool_id))

    if master_node_id == config.node_id:
        return

    while True:
        master_node = get_node(master_node_id)

        if master_node.state in [batchmodels.ComputeNodeState.idle, batchmodels.ComputeNodeState.running]:
            break
        else:
            print("{0} Still waiting on master", datetime.datetime.now())
            time.sleep(10)


def start_hadoop_master():
    master_ip = get_node(config.node_id).ip_address
    exe = os.path.join(hadoop_home, "sbin", "start-master.sh")
    cmd = [exe, "-h", master_ip, "--webui-port",
           str(config.hadoop_web_ui_port)]
    print("Starting master with '{0}'".format(" ".join(cmd)))
    call(cmd)
    try:
        start_history_server()
    except Exception as e:
        print("Failed to start history server with the following exception:")
        print(e)


def start_hadoop_worker():
    wait_for_master()
    exe = os.path.join(hadoop_home, "sbin", "start-slave.sh")
    master_node_id = pick_master.get_master_node_id(
        batch_client.pool.get(config.pool_id))
    master_node = get_node(master_node_id)

    cmd = [exe, "hadoop://{0}:7077".format(master_node.ip_address),
           "--webui-port", str(config.hadoop_worker_ui_port)]
    print("Connecting to master with '{0}'".format(" ".join(cmd)))
    call(cmd)


def copyfile(src, dest):
    try:
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copyfile(src, dest)
        file_stat = os.stat(dest)
        os.chmod(dest, file_stat.st_mode | 0o777)
    except Exception as e:
        print("Failed to copy", src)
        print(e)


def setup_conf():
    """
        Copy hadoop conf files to hadoop_home if they were uploaded
    """
    copy_hadoop_env()
    copy_core_site()
    copy_hadoop_defaults()
    copy_jars()
    setup_ssh_keys()


def setup_ssh_keys():
    pub_key_path_src = os.path.join(os.environ['AZTK_WORKING_DIR'], 'id_rsa.pub')
    priv_key_path_src = os.path.join(os.environ['AZTK_WORKING_DIR'], 'id_rsa')
    ssh_key_dest = '/root/.ssh'

    if not os.path.exists(ssh_key_dest):
        os.mkdir(ssh_key_dest)

    copyfile(pub_key_path_src, os.path.join(ssh_key_dest, os.path.basename(pub_key_path_src)))
    copyfile(priv_key_path_src, os.path.join(ssh_key_dest, os.path.basename(priv_key_path_src)))


def copy_hadoop_env():
    hadoop_env_path_src = os.path.join(os.environ['AZTK_WORKING_DIR'], 'conf/hadoop-env.sh')
    hadoop_env_path_dest = os.path.join(hadoop_home, 'conf/hadoop-env.sh')
    copyfile(hadoop_env_path_src, hadoop_env_path_dest)


def copy_hadoop_defaults():
    hadoop_default_path_src = os.path.join(os.environ['AZTK_WORKING_DIR'], 'conf/hadoop-defaults.conf')
    hadoop_default_path_dest = os.path.join(hadoop_home, 'conf/hadoop-defaults.conf')
    copyfile(hadoop_default_path_src, hadoop_default_path_dest)


def copy_core_site():
    hadoop_core_site_src = os.path.join(os.environ['AZTK_WORKING_DIR'], 'conf/core-site.xml')
    hadoop_core_site_dest = os.path.join(hadoop_home, 'conf/core-site.xml')
    copyfile(hadoop_core_site_src, hadoop_core_site_dest)


def copy_jars():
    # Copy jars to $hadoop_HOME/jars
    hadoop_default_path_src = os.path.join(os.environ['AZTK_WORKING_DIR'], 'jars')
    hadoop_default_path_dest = os.path.join(hadoop_home, 'jars')

    try:
        jar_files = os.listdir(hadoop_default_path_src)
        for jar in jar_files:
            src = os.path.join(hadoop_default_path_src, jar)
            dest = os.path.join(hadoop_default_path_dest, jar)
            print("copy {} to {}".format(src, dest))
            copyfile(src, dest)
    except Exception as e:
        print("Failed to copy jar files with error:")
        print(e)


def parse_configuration_file(path_to_file: str):
    try:
        file = open(path_to_file, 'r', encoding='UTF-8')
        properties = {}
        for line in file:
            if (not line.startswith('#') and len(line) > 1):
                split = line.split()
                properties[split[0]] = split[1]
        return properties
    except Exception as e:
        print("Failed to parse configuration file:", path_to_file, "with error:")
        print(e)


def start_history_server():
    # configure the history server
    hadoop_event_log_enabled_key = 'hadoop.eventLog.enabled'
    hadoop_event_log_directory_key = 'hadoop.eventLog.dir'
    hadoop_history_fs_log_directory = 'hadoop.history.fs.logDirectory'
    path_to_hadoop_defaults_conf = os.path.join(hadoop_home, 'conf/hadoop-defaults.conf')
    properties = parse_configuration_file(path_to_hadoop_defaults_conf)
    required_keys = [hadoop_event_log_enabled_key, hadoop_event_log_directory_key, hadoop_history_fs_log_directory]

    # only enable the history server if it was enabled in the configuration file
    if properties:
        if all(key in properties for key in required_keys):
            configure_history_server_log_path(properties[hadoop_history_fs_log_directory])
            exe = os.path.join(hadoop_home, "sbin", "start-history-server.sh")
            print("Starting history server")
            call([exe])


def configure_history_server_log_path(path_to_log_file):
    # Check if the file path starts with a local file extension
    # If so, create the path on disk otherwise ignore
    print('Configuring hadoop history server log directory {}.'.format(path_to_log_file))
    if path_to_log_file.startswith('file:/'):
        # create the local path on disk
        directory = path_to_log_file.replace('file:', '')
        if os.path.exists(directory):
            print('Skipping. Directory {} already exists.'.format(directory))
        else:
            print('Create directory {}.'.format(directory))
            os.makedirs(directory)

            # Make sure the directory can be accessed by all users
            os.chmod(directory, mode=0o777)
    else:
        print('Skipping. The eventLog directory is not local.')
