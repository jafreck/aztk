from typing import List
import aztk_sdk.models
from aztk_sdk.utils import constants
import azure.batch.models as batch_models


class Cluster(aztk_sdk.models.Cluster):
    def __init__(self, pool: batch_models.CloudPool, nodes: batch_models.ComputeNodePaged = None):
        super().__init__(pool, nodes)
        self.master_node_id = self.__get_master_node_id()

    def is_pool_running_dask(self, pool: batch_models.CloudPool):
        if pool.metadata is None:
            return False

        for metadata in pool.metadata:
            if metadata.name == constants.AZTK_SOFTWARE_METADATA_KEY:
                return metadata.value == aztk_sdk.models.Software.dask

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

class RemoteLogin(aztk_sdk.models.RemoteLogin):
    pass

class DaskConfiguration():
    def __init__(self, jars: List[str]=None):
        self.jars = jars

class CustomScript(aztk_sdk.models.CustomScript):
    pass


class ClusterConfiguration(aztk_sdk.models.ClusterConfiguration):
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
        self.dask_configuration = dask_configuration


class SecretsConfiguration(aztk_sdk.models.SecretsConfiguration):
    pass


class VmImage(aztk_sdk.models.VmImage):
    pass


class ProgramModel:
    def __init__(
            self,
            name=None,
            application=None,
            application_args=None,
            main_class=None,
            jars=[],
            py_files=[],
            files=[],
            driver_java_options=None,
            driver_library_path=None,
            driver_class_path=None,
            driver_memory=None,
            executor_memory=None,
            driver_cores=None,
            executor_cores=None):
        self.name = name
        self.application = application
        self.application_args = application_args


class ProgramLogsModel():
    def __init__(self, name: str, cluster_id: str, log: str, total_bytes: int, application_state: batch_models.TaskState):
        self.name = name
        self.cluster_id = cluster_id
        self.log = log
        self.total_bytes = total_bytes
        self.application_state = application_state
