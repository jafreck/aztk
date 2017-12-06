from typing import List
import azure.batch.models.batch_error as batch_error
import aztk_sdk
from aztk_sdk import error
from aztk_sdk.client import Client as BaseClient
from aztk_sdk.spark import models
from aztk_sdk.utils import helpers
from aztk_sdk.spark.helpers import create_cluster as create_cluster_helper
from aztk_sdk.spark.helpers import submit as submit_helper
from aztk_sdk.spark.helpers import job_submission as job_submit_helper
from aztk_sdk.spark.helpers import get_log as get_log_helper
from aztk_sdk.spark.utils import upload_node_scripts, util


class Client(BaseClient):
    def __init__(self, secrets_config):
        super().__init__(secrets_config)

    '''
    Spark client public interface
    '''
    def create_cluster(self, cluster_conf: models.ClusterConfiguration, wait: bool = False):
        try:
            zip_resource_files = upload_node_scripts.zip_scripts(self.blob_client,
                                                                 cluster_conf.cluster_id,
                                                                 cluster_conf.custom_scripts,
                                                                 cluster_conf.spark_configuration)
            start_task = create_cluster_helper.generate_cluster_start_task(self, zip_resource_files, cluster_conf.docker_repo)

            software_metadata_key = "spark"

            vm_image = models.VmImage(
                publisher='Canonical',
                offer='UbuntuServer',
                sku='16.04')

            cluster = self.__create_pool_and_job(
                cluster_conf, software_metadata_key, start_task, vm_image)
            
            # Wait for the master to be ready
            if wait:
                util.wait_for_master_to_be_ready(self, cluster.id)
                cluster = self.get_cluster(cluster.id)
            
            return cluster
        
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))
    
    def create_clusters_in_parallel(self, cluster_confs):
        for cluster_conf in cluster_confs:
            self.create_cluster(cluster_conf)

    def delete_cluster(self, cluster_id: str):
        try:
            return self.__delete_pool_and_job(cluster_id)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))

    def get_cluster(self, cluster_id: str):
        try:
            pool, nodes = self.__get_pool_details(cluster_id)
            return models.Cluster(pool, nodes)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))
    
    def list_clusters(self):
        try:
            return [models.Cluster(pool) for pool in self.__list_clusters(aztk_sdk.models.Software.spark)]
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))

    def get_remote_login_settings(self, cluster_id: str, node_id: str):
        try:
            return self.__get_remote_login_settings(cluster_id, node_id)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))

    def submit(self, cluster_id: str, application: models.AppModel, wait: bool = False):
        try:
            submit_helper.submit_application(self, cluster_id, application, wait)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))
    
    def submit_all_applications(self, cluster_id: str, applications):
        for application in applications:
            self.submit(cluster_id, application)
    
    def wait_until_application_done(self, cluster_id: str, task_id: str):
        try:
            helpers.wait_for_task_to_complete(job_id=cluster_id, task_id=task_id, batch_client=self.batch_client)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))

    def wait_until_applications_done(self, cluster_id: str):
        try:
            helpers.wait_for_tasks_to_complete(job_id=cluster_id, batch_client=self.batch_client)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))

    def wait_until_cluster_is_ready(self, cluster_id: str):
        try:
            util.wait_for_master_to_be_ready(self, cluster_id)
            pool = self.batch_client.pool.get(cluster_id)
            nodes = self.batch_client.compute_node.list(pool_id=cluster_id)
            return models.Cluster(pool, nodes)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))
    
    def wait_until_all_clusters_are_ready(self, clusters: List[str]):
        for cluster_id in clusters:
            self.wait_until_cluster_is_ready(cluster_id)

    def create_user(self, cluster_id: str, username: str, password: str = None, ssh_key: str = None) -> str:
        try:
            cluster = self.get_cluster(cluster_id)
            master_node_id = cluster.master_node_id
            self.__create_user(cluster.id, master_node_id, username, password, ssh_key)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))

    def get_application_log(self, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
        try:
            return get_log_helper.get_log(self.batch_client, cluster_id, application_name, tail, current_bytes)
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))

    def get_application_status(self, cluster_id: str, app_name: str):
        try:
            task = self.batch_client.task.get(cluster_id, app_name)
            return task.state._value_
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))
    
    def submit_job(self, job):
        try:
            zip_resource_files = upload_node_scripts.zip_scripts(self.blob_client, job.id, job.custom_scripts, job.spark_configuration)
            start_task = create_cluster_helper.generate_cluster_start_task(self, zip_resource_files, job.docker_repo) #TODO add job.gpu_enabled

            # job.application.gpu_enabled = job.gpu_enabled

            # TODO: 
            #   create a  task for all applications in job.applications
            #       1. upload resource fiels and create task: batch_models.TaskAddParameter
            #       2. searialize(pickle) the tasks (their resource files will already have been uploaded)
            #           2a. Look at where the resource files are uploaded and possible change location
            #       3. Add these serialized tasks as resource files for the Job Manager Task
            #   make a job manager task that:
            #       1. zips and uploads the serialized tasks it needs to schedule
            #       2. On node:
            #           2a. Unzip and deserialize tasks
            #           2b. create a batch_client
            #           2c. schedule tasks (which already have their resource files uploaded)
             
            application_tasks = []
            for application in job.applications:
                application_tasks.append((application, submit_helper.generate_task(self, job.id, application)))

            job_manager_task = job_submit_helper.generate_task(self, job, application_tasks)


            software_metadata_key = "spark"

            vm_image = models.VmImage(
                publisher='Canonical',
                offer='UbuntuServer',
                sku='16.04')
            
            autoscale_formula = "maxNumberofVMs = {0}; targetNumberofVMs = {1}; $TargetDedicatedNodes=min(maxNumberofVMs, targetNumberofVMs)".format(
                job.max_dedicated_nodes, job.max_dedicated_nodes)

            job = self.__submit_job(
                job=job,
                start_task=start_task,
                job_manager_task=job_manager_task,
                autoscale_formula=autoscale_formula,
                software_metadata_key=software_metadata_key,
                vm_image_model=vm_image)
        
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))


    def get_job_log(self, job):
        try:
            log_file = aztk_sdk.utils.constants.SPARK_SUBMIT_LOGS_FILE
            return self.blob_client.get_blob_to_text(job.application.name, log_file).content
        except batch_error.BatchErrorException as e:
            raise error.AztkError(helpers.format_batch_exception(e))