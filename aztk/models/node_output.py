# from aztk.core.models import Model, fields

# class NodeOutput:
#     id = fields.String()
#     output = fields.Field()
#     error = fields.Field()

from tempfile import SpooledTemporaryFile
from  typing import Union

class NodeOutput:
    def __init__(self, id: str, output: Union[SpooledTemporaryFile, str] = None, error: Exception = None):
        self.id = id
        self.output = output
        self.error = error
