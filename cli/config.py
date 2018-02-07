import os
import yaml
import typing
from cli import log
import aztk.spark
from aztk.models import ServicePrincipalConfiguration, SharedKeyConfiguration, DockerConfiguration
from aztk.spark.models import SecretsConfiguration


def load_aztk_screts() -> SecretsConfiguration:
    """
    Loads aztk from .aztk/secrets.yaml files(local and global)
    """
    secrets = SecretsConfiguration()
    # read global ~/secrets.yaml
    global_config = _load_secrets_config(os.path.join(
        aztk.utils.constants.HOME_DIRECTORY_PATH, '.aztk', 'secrets.yaml'))
    # read current working directory secrets.yaml
    local_config = _load_secrets_config()

    if not global_config and not local_config:
        raise "There is no secrets.yaml in either ./.aztk/secrets.yaml or .aztk/secrets.yaml"

    if global_config:  # GLobal config is optional
        _merge_secrets_dict(secrets, global_config)
    if local_config:
        _merge_secrets_dict(secrets, local_config)

    # Validate and raise error if any
    secrets.validate()
    return secrets


def _load_secrets_config(path: str = aztk.utils.constants.DEFAULT_SECRETS_PATH):
    """
        Loads the secrets.yaml file in the .aztk directory
    """
    if not os.path.isfile(path):
        return None

    with open(path, 'r') as stream:
        try:
            return yaml.load(stream)
        except yaml.YAMLError as err:
            raise aztk.error.AztkError(
                "Error in secrets.yaml: {0}".format(err))


def _merge_secrets_dict(secrets: SecretsConfiguration, secrets_config):
    service_principal_config = secrets_config.get('service_principal')
    if service_principal_config:
        secrets.service_principal = ServicePrincipalConfiguration(
            tenant_id=service_principal_config.get('tenant_id'),
            client_id=service_principal_config.get('client_id'),
            credential=service_principal_config.get('credential'),
            batch_account_resource_id=service_principal_config.get(
                'batch_account_resource_id'),
            storage_account_resource_id=service_principal_config.get(
                'storage_account_resource_id'),
        )

    shared_key_config = secrets_config.get('shared_key')
    batch = secrets_config.get('batch')
    storage = secrets_config.get('storage')

    if shared_key_config and (batch or storage):
        raise aztk.error.AztkError(
            "Shared keys must be configured either under 'sharedKey:' or under 'batch:' and 'storage:', not both.")

    if shared_key_config:
        secrets.shared_key = SharedKeyConfiguration(
            batch_account_name=shared_key_config.get('batch_account_name'),
            batch_account_key=shared_key_config.get('batch_account_key'),
            batch_service_url=shared_key_config.get('batch_service_url'),
            storage_account_name=shared_key_config.get(
                'storage_account_name'),
            storage_account_key=shared_key_config.get(
                'storage_account_key'),
            storage_account_suffix=shared_key_config.get(
                'storage_account_suffix'),
        )
    elif batch or storage:
        secrets.shared_key = SharedKeyConfiguration()
        if batch:
            log.warning(
                "Your secrets.yaml format is deprecated. To use shared key authentication use the shared_key key. See config/secrets.yaml.template")
            secrets.shared_key.batch_account_name = batch.get(
                'batchaccountname')
            secrets.shared_key.batch_account_key = batch.get(
                'batchaccountkey')
            secrets.shared_key.batch_service_url = batch.get(
                'batchserviceurl')

        if storage:
            secrets.shared_key.storage_account_name = storage.get(
                'storageaccountname')
            secrets.shared_key.storage_account_key = storage.get(
                'storageaccountkey')
            secrets.shared_key.storage_account_suffix = storage.get(
                'storageaccountsuffix')

    docker_config = secrets_config.get('docker')
    if docker_config:
        secrets.docker = DockerConfiguration(
            endpoint = docker_config.get('endpoint'),
            username = docker_config.get('username'),
            password = docker_config.get('password'),
        )

    default_config = secrets_config.get('default')
    # Check for ssh keys if they are provided
    if default_config:
        secrets.ssh_priv_key = default_config.get('ssh_priv_key')
        secrets.ssh_pub_key = default_config.get('ssh_pub_key')


class ClusterConfig:

    def __init__(self):
        self.uid = None
        self.vm_size = None
        self.size = 0
        self.size_low_pri = 0
        self.subnet_id = None
        self.username = None
        self.password = None
        self.custom_scripts = None
        self.file_shares = None
        self.docker_repo = None
        self.wait = None
        self.mixed_mode = False

    def _read_config_file(self, path: str = aztk.utils.constants.DEFAULT_CLUSTER_CONFIG_PATH):
        """
            Reads the config file in the .aztk/ directory (.aztk/cluster.yaml)
        """
        if not os.path.isfile(path):
            return

        with open(path, 'r') as stream:
            try:
                config = yaml.load(stream)
            except yaml.YAMLError as err:
                raise aztk.error.AztkError(
                    "Error in cluster.yaml: {0}".format(err))

            if config is None:
                return

            self._merge_dict(config)

    def _merge_dict(self, config):
        if config.get('id') is not None:
            self.uid = config['id']

        if config.get('vm_size') is not None:
            self.vm_size = config['vm_size']

        if config.get('size') is not None:
            self.size = config['size']

        if config.get('size_low_pri') is not None:
            self.size_low_pri = config['size_low_pri']

        if config.get('subnet_id') is not None:
            self.subnet_id = config['subnet_id']

        if config.get('username') is not None:
            self.username = config['username']

        if config.get('password') is not None:
            self.password = config['password']

        if config.get('custom_scripts') not in [[None], None]:
            self.custom_scripts = config['custom_scripts']

        if config.get('azure_files') not in [[None], None]:
            self.file_shares = config['azure_files']

        if config.get('docker_repo') is not None:
            self.docker_repo = config['docker_repo']

        if config.get('wait') is not None:
            self.wait = config['wait']

    def merge(self, spark_client, uid, username, size, size_low_pri, vm_size, subnet_id, password, wait, docker_repo):
        """
            Reads configuration file (cluster.yaml), merges with command line parameters,
            checks for errors with configuration
        """
        self._read_config_file(os.path.join(
            aztk.utils.constants.HOME_DIRECTORY_PATH, '.aztk', 'cluster.yaml'))
        self._read_config_file()

        self._merge_dict(
            dict(
                id=uid,
                username=username,
                size=size,
                size_low_pri=size_low_pri,
                vm_size=vm_size,
                subnet_id=subnet_id,
                password=password,
                wait=wait,
                custom_scripts=None,
                docker_repo=docker_repo
            )
        )

        if self.uid is None:
            raise aztk.error.AztkError(
                "Please supply an id for the cluster with a parameter (--id)")

        if self.size == 0 and self.size_low_pri == 0:
            raise aztk.error.AztkError(
                "Please supply a valid (greater than 0) size or size_low_pri value either in the cluster.yaml configuration file or with a parameter (--size or --size-low-pri)")

        if self.vm_size is None:
            raise aztk.error.AztkError(
                "Please supply a vm_size in either the cluster.yaml configuration file or with a parameter (--vm-size)")

        if self.wait is None:
            raise aztk.error.AztkError(
                "Please supply a value for wait in either the cluster.yaml configuration file or with a parameter (--wait or --no-wait)")

        if self.size > 0 and self.size_low_pri > 0:
            self.mixed_mode = True

        if not spark_client.secrets_config.service_principal.tenant_id and self.mixed_mode:
            raise aztk.error.AztkError(
                "You must configure an AAD service principal to use AZTK in mixed mode (dedicated and low priority nodes).")

        if not self.subnet_id and self.mixed_mode:
            raise aztk.error.AztkError(
                "You must configure a VNET to use AZTK in mixed mode (dedicated and low priority nodes). Set the VNET's subnet_id in your cluster.yaml.")

class SshConfig:

    def __init__(self):
        self.username = None
        self.cluster_id = None
        self.host = False
        self.connect = True

        # Set up ports with default values
        self.job_ui_port = '4040'
        self.job_history_ui_port = '18080'
        self.web_ui_port = '8080'
        self.jupyter_port = '8888'
        self.name_node_ui_port = '50070'
        self.rstudio_server_port = '8787'

    def _read_config_file(self, path: str = aztk.utils.constants.DEFAULT_SSH_CONFIG_PATH):
        """
            Reads the config file in the .aztk/ directory (.aztk/ssh.yaml)
        """
        if not os.path.isfile(path):
            return

        with open(path, 'r') as stream:
            try:
                config = yaml.load(stream)
            except yaml.YAMLError as err:
                raise aztk.error.AztkError(
                    "Error in ssh.yaml: {0}".format(err))

            if config is None:
                return

            self._merge_dict(config)

    def _merge_dict(self, config):
        if config.get('username') is not None:
            self.username = config['username']

        if config.get('cluster_id') is not None:
            self.cluster_id = config['cluster_id']

        if config.get('job_ui_port') is not None:
            self.job_ui_port = config['job_ui_port']

        if config.get('job_history_ui_port') is not None:
            self.job_history_ui_port = config['job_history_ui_port']

        if config.get('web_ui_port') is not None:
            self.web_ui_port = config['web_ui_port']

        if config.get('jupyter_port') is not None:
            self.jupyter_port = config['jupyter_port']

        if config.get('name_node_ui_port') is not None:
            self.name_node_ui_port = config['name_node_ui_port']

        if config.get('rstudio_server_port') is not None:
            self.rstudio_server_port = config['rstudio_server_port']

        if config.get('host') is not None:
            self.host = config['host']

        if config.get('connect') is not None:
            self.connect = config['connect']

    def merge(self, cluster_id, username, job_ui_port, job_history_ui_port, web_ui_port, jupyter_port, name_node_ui_port, rstudio_server_port, host, connect):
        """
            Merges fields with args object
        """
        self._read_config_file(os.path.join(aztk.utils.constants.HOME_DIRECTORY_PATH, '.aztk', 'ssh.yaml'))
        self._read_config_file()
        self._merge_dict(
            dict(
                cluster_id=cluster_id,
                username=username,
                job_ui_port=job_ui_port,
                job_history_ui_port=job_history_ui_port,
                web_ui_port=web_ui_port,
                jupyter_port=jupyter_port,
                name_node_ui_port=name_node_ui_port,
                rstudio_server_port=rstudio_server_port,
                host=host,
                connect=connect
            )
        )

        if self.cluster_id is None:
            raise aztk.error.AztkError(
                "Please supply an id for the cluster either in the ssh.yaml configuration file or with a parameter (--id)")

        if self.username is None:
            raise aztk.error.AztkError(
                "Please supply a username either in the ssh.yaml configuration file or with a parameter (--username)")

class JobConfig():
    def __init__(self):
        self.id = None
        self.applications = []
        self.custom_scripts = None
        self.spark_configuration = None
        self.vm_size=None
        self.docker_repo = None
        self.max_dedicated_nodes = None
        self.max_low_pri_nodes = None
        self.spark_defaults_conf = None
        self.spark_env_sh = None
        self.core_site_xml = None
        self.subnet_id = None

    def _merge_dict(self, config):
        config = config.get('job')

        if config.get('id') is not None:
            self.id = config['id']

        cluster_configuration = config.get('cluster_configuration')
        if cluster_configuration:
            self.vm_size = cluster_configuration.get('vm_size')
            self.docker_repo = cluster_configuration.get('docker_repo')
            self.max_dedicated_nodes = cluster_configuration.get('size')
            self.max_low_pri_nodes = cluster_configuration.get('size_low_pri')
            self.custom_scripts = cluster_configuration.get('custom_scripts')
            self.subnet_id = cluster_configuration.get('subnet_id')

        self.applications = config.get('applications')

        spark_configuration = config.get('spark_configuration')
        if spark_configuration:
            self.spark_defaults_conf = self.__convert_to_path(spark_configuration.get('spark_defaults_conf'))
            self.spark_env_sh = self.__convert_to_path(spark_configuration.get('spark_env_sh'))
            self.core_site_xml = self.__convert_to_path(spark_configuration.get('core_site_xml'))
            self.jars = [self.__convert_to_path(jar) for jar in spark_configuration.get('jars')]

    def __convert_to_path(self, str_path):
        if str_path:
            abs_path = os.path.abspath(os.path.expanduser(str_path))
            if not os.path.exists(abs_path):
                raise aztk.error.AztkError("Could not find file: {0}\nCheck your configuration file".format(str_path))
            return abs_path

    def _read_config_file(self, path: str = aztk.utils.constants.DEFAULT_SPARK_JOB_CONFIG):
        """
            Reads the Job config file in the .aztk/ directory (.aztk/job.yaml)
        """
        if not path or not os.path.isfile(path):
            return

        with open(path, 'r') as stream:
            try:
                config = yaml.load(stream)
            except yaml.YAMLError as err:
                raise aztk.error.AztkError(
                    "Error in job.yaml: {0}".format(err))

            if config is None:
                return

            self._merge_dict(config)

    def merge(self, id, job_config_yaml=None):
        self._read_config_file(aztk.utils.constants.GLOBAL_SPARK_JOB_CONFIG)
        self._read_config_file(aztk.utils.constants.DEFAULT_SPARK_JOB_CONFIG)
        self._read_config_file(job_config_yaml)
        if id:
            self.id = id

        for entry in self.applications:
            if entry['name'] is None:
                raise aztk.error.AztkError(
                    "Application specified with no name. Please verify your configuration in job.yaml")
            if entry['application'] is None:
                raise aztk.error.AztkError(
                    "No path to application specified for {} in job.yaml".format(entry['name']))


def get_file_if_exists(file):
    local_conf_file = os.path.join(aztk.utils.constants.DEFAULT_SPARK_CONF_SOURCE, file)
    global_conf_file = os.path.join(aztk.utils.constants.GLOBAL_CONFIG_PATH, file)

    if os.path.exists(local_conf_file):
        return local_conf_file
    if os.path.exists(global_conf_file):
        return global_conf_file

    return None


def load_aztk_spark_config():
    return aztk.spark.models.SparkConfiguration(
        spark_defaults_conf=get_file_if_exists('spark-defaults.conf'),
        jars=load_jars(),
        spark_env_sh=get_file_if_exists('spark-env.sh'),
        core_site_xml=get_file_if_exists('core-site.xml'))


def load_jars():
    jars = None

    # try load global
    try:
        jars_src = os.path.join(
            aztk.utils.constants.GLOBAL_CONFIG_PATH, 'jars')
        jars = [os.path.join(jars_src, jar) for jar in os.listdir(jars_src)]
    except FileNotFoundError:
        pass

    # try load local, overwrite if found
    try:
        jars_src = os.path.join(
            aztk.utils.constants.DEFAULT_SPARK_CONF_SOURCE, 'jars')
        jars = [os.path.join(jars_src, jar) for jar in os.listdir(jars_src)]
    except FileNotFoundError:
        pass

    return jars
