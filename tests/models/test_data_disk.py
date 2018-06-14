import pytest

from aztk.models import DataDisk, DataDiskFormatType
from aztk.error import InvalidModelError


def test_valid_data_disk():
    data_disk = DataDisk(disk_size_gb=10, mount_path='/test/path', format_type=DataDiskFormatType.ext2)
    data_disk.validate()

    assert data_disk.disk_size_gb == 10
    assert data_disk.mount_path == '/test/path'
    assert data_disk.format_type == DataDiskFormatType.ext2


def test_uninitialized_data_disk():
    data_disk = DataDisk()
    with pytest.raises(InvalidModelError):
        data_disk.validate()

    assert data_disk.disk_size_gb is None
    assert data_disk.mount_path is None
    assert data_disk.format_type == DataDiskFormatType.ext4


def test_data_disk_minimum_required_fields():
    data_disk = DataDisk(disk_size_gb=1)
    assert data_disk.disk_size_gb == 1
    assert data_disk.mount_path == None
    assert data_disk.format_type == DataDiskFormatType.ext4


def test_data_disk_format_type():
    data_disk = DataDisk(disk_size_gb=1, format_type=DataDiskFormatType.ext2)
    assert data_disk.format_type == "ext2"
    assert data_disk.format_type == DataDiskFormatType.ext2

    data_disk = DataDisk(disk_size_gb=1, format_type="ext2")
    assert data_disk.format_type == "ext2"
    assert data_disk.format_type == DataDiskFormatType.ext2
