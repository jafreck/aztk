from contextlib import contextmanager

from azure.batch.models import BatchErrorException

from aztk import error
from aztk.utils import constants, helpers


@contextmanager
def batch_error_manager():
    try:
        yield
    except BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
