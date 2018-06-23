from aztk.client.base import BaseClient
from .helpers import (
    submit, )


class Client(BaseClient):
    def submit(self, job_configuration, start_task, job_manager_task, autoscale_formula, software_metadata_key: str,
               vm_image_model, application_metadata):
        return submit.submit_job(self, job_configuration, start_task, job_manager_task, autoscale_formula,
                                 software_metadata_key, vm_image_model, application_metadata)
