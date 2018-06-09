from aztk.core.models import Model, fields

from .data_disk_format_type import DataDiskFormatType


class DataDisk(Model):
    disk_size_gb = fields.Integer()
    mount_path = fields.String()
    format_type = fields.String(default=DataDiskFormatType.ext4)
