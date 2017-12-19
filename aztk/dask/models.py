from Crypto.PublicKey import RSA
from typing import List
import aztk.models
from aztk.utils import constants, helpers
import azure.batch.models as batch_models


class Cluster(aztk.models.Cluster):
    def __init__(self, pool: batch_models.CloudPool, nodes: batch_models.ComputeNodePaged = None):
        super().__init__(pool, nodes)
        self.master_node_id = self.__get_master_node_id()

    def is_pool_running_dask(self, pool: batch_models.CloudPool):
        if pool.metadata is None:
            return False

        for metadata in pool.metadata:
            if metadata.name == constants.AZTK_SOFTWARE_METADATA_KEY:
                return metadata.value == aztk.models.Software.dask

        return False

    def __get_master_node_id(self):
        """
            :returns: the id of the node that is the assigned master of this pool
        """
        if self.pool.metadata is None:
            return None

        for metadata in self.pool.metadata:
            if metadata.name == constants.DASK_MASTER_NODE_METADATA_KEY:
                return metadata.value

        return None

class RemoteLogin(aztk.models.RemoteLogin):
    pass

class DaskConfiguration():
    def __init__(self, jars: List[str]=None):
        self.jars = jars
        self.ssh_key_pair = self.__generate_ssh_key_pair()
    
    def __generate_ssh_key_pair(self):
        key = RSA.generate(2048)
        priv_key = key.exportKey('PEM')
        pub_key = key.publickey().exportKey('OpenSSH')
        return {'pub_key': pub_key, 'priv_key': priv_key}

class CustomScript(aztk.models.CustomScript):
    pass


class ClusterConfiguration(aztk.models.ClusterConfiguration):
    def __init__(
            self,
            custom_scripts: List[CustomScript] = None,
            cluster_id: str = None,
            vm_count=None,
            vm_low_pri_count=None,
            vm_size=None,
            docker_repo: str=None,
            dask_configuration: DaskConfiguration=None):
        super().__init__(custom_scripts=custom_scripts,
              cluster_id=cluster_id,
              vm_count=vm_count,
              vm_low_pri_count=vm_low_pri_count,
              vm_size=vm_size,
              docker_repo=docker_repo
        )
        self.gpu_enabled = helpers.is_gpu_enabled(vm_size)
        self.dask_configuration = dask_configuration


class SecretsConfiguration(aztk.models.SecretsConfiguration):
    pass


class VmImage(aztk.models.VmImage):
    pass


class Application:
    def __init__(
            self,
            name=None,
            application=None,
            application_args=None,
            files=[]):
        self.name = name
        self.application = application
        self.application_args = application_args
        self.files = files


class ProgramLog():
    def __init__(self, name: str, cluster_id: str, log: str, total_bytes: int, application_state: batch_models.TaskState):
        self.name = name
        self.cluster_id = cluster_id
        self.log = log
        self.total_bytes = total_bytes
        self.application_state = application_state
