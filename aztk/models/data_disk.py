from aztk.core.models import Model, fields

from .data_disk_format_type import DataDiskFormatType

class DataDisk(Model):
    """
    Configuration for an additional local storage disk that is attached to the virtual machine,
        formatted and mounted into the Spark Docker container

    Args:
        disk_size_gb (int): Which docker endpoint to use. Default to docker hub.
        mount_path (:obj:`str`, optional): the path where the disk should be mounted
        format_type (:obj:`aztk.models.DataDiskFormatType`, optional): the type of file system format
    """
    disk_size_gb = fields.Integer()
    mount_path = fields.String()
    format_type = fields.String(default=DataDiskFormatType.ext4)
