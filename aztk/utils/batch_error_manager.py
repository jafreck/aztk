import traceback
from contextlib import contextmanager

from azure.batch.models import BatchErrorException

from aztk import error
from aztk.utils import helpers


@contextmanager
def batch_error_manager(verbose=False):
    try:
        yield
    except BatchErrorException as e:
        if verbose:
            # TODO: change to log.debug
            print(traceback.format_exc())
        raise error.AztkError(helpers.format_batch_exception(e))
