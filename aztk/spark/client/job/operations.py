from aztk.client.job import CoreJobOperations
from aztk.spark import models
from aztk.spark.client.base import SparkBaseOperations

from .helpers import (delete, get, get_application, get_application_log, list, list_applications, stop,
                      stop_application, submit, wait_until_complete)


class JobOperations(CoreJobOperations, SparkBaseOperations):
    def list(self):
        """List all jobs.

        Returns:
            List[Job]: List of aztk.models.Job objects each representing the state and configuration of the job.
        """
        return list.list_jobs(self)

    def delete(self, id, keep_logs: bool = False):
        """Delete a job.

        Args:
            id (:obj:`str`): the id of the cluster to delete.
            keep_logs (:obj:`bool`): If True, the logs related to this cluster in Azure Storage are not deleted.
                Defaults to False.
        Returns:
            True if the deletion process was successful.
        """
        return delete.delete(self, id, keep_logs)

    def get(self, id):
        """Get details about the state of a job.

        Args:
            id (:obj:`str`): the id of the cluster to get.

        Returns:
            Cluster: An aztk.models.Cluster object representing the state and configuration of the cluster.
        """
        return get.get_job(self, id)

    def get_application(self, id, application_name):
        """Get information on a submitted application

        Args:
            id (:obj:`str`): the name of the cluster the application was submitted to
            application_name (:obj:`str`): the name of the application to get

        Returns:
            aztk.spark.models.Application: object representing that state and output of an application
        """
        return get_application.get_application(self, id, application_name)

    def get_application_log(self, id, application_name):
        """Get the log for a running or completed application

        Args:
            id (:obj:`str`): the id of the cluster to run the command on.
            application_name (:obj:`str`): str

        Returns:
            aztk.spark.models.ApplicationLog: a model representing the output of the application.
        """
        return get_application_log.get_job_application_log(self, id, application_name)

    def list_applications(self, id):
        """List all application defined as a part of a job
        
        Args:
            id (:obj:`str`): the id of the job to list the applications of
        
        Returns:
            List[aztk.spark.models.Application]: a list of all applications defined as a part of the job
        """
        return list_applications.list_applications(self, id)

    def stop(self, id):
        """Stop a submitted job

        Args:
            id (:obj:`str`): the id of the job to stop
        
        Returns:
            None
        """
        return stop.stop(self, id)

    def stop_application(self, id, application_name):
        """Stops a submitted application

        Args:
            id (:obj:`str`): the id of the job the application belongs to
            application_name (:obj:`str`):  the name of the application to stop
        
        Returns:
            bool: True if the stop was successful, else False
        """
        return stop_application.stop_app(self, id, application_name)

    def submit(self, job_configuration: models.JobConfiguration):
        """Submit a job

        Jobs are a cluster definition and one or many application definitions which run on the cluster. The job's
        cluster will be allocated and configured, then the applications will be executed with their output stored
        in Azure Storage. When all applications have completed, the cluster will be automatically deleted.

        Args:
            job_configuration (:obj:`aztk.models.JobConfiguration`): Model defining the job's configuration.

        Returns:
            aztk.spark.models.Job: Model representing the state of the job.
        """
        return submit.submit_job(self, job_configuration)

    def wait_until_job_finished(self, id):    #TODO: rename to something better
        wait_until_complete.wait_until_job_finished(self, id)
