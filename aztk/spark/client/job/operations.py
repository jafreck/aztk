from aztk.spark import models
from aztk.spark.client.base import SparkBaseOperations

from .helpers import (delete, get, get_application, get_application_log, list, list_applications, stop, submit)


class JobOperations(SparkBaseOperations):
    def list(self):
        return list.list_jobs(self)

    def delete(self, id, keep_logs: bool = False):
        return delete.delete(self, id, keep_logs)

    def get(self, id):
        return get.get_job(self, id)

    def get_application(self, id, application_name):
        return get_application.get_application(self, id, application_name)

    def get_application_log(self, id, application_name):
        return get_application_log.get_job_application_log(self, id, application_name)

    def list_applications(self, id):
        return list_applications.list_applications(self, id)

    def stop(self, id):
        return stop.stop(self, id)

    def submit(self, job_configuration: models.JobConfiguration):
        return submit.submit_job(self, job_configuration)
