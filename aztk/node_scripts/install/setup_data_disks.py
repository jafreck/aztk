import subprocess
import sys
import os


def mount_data_disk(data_disk, device_name, number):
    cmd = os.environ["AZTK_WORKING_DIR"] + "/aztk/node_scripts/install/setup_data_disk.sh "
    data_disk.mount_path = "/data-disk" + str(number) if not data_disk.mount_path else data_disk.mount_path
    args = device_name + " " + data_disk.format_type + " " + data_disk.mount_path
    cmd = cmd + args
    print("mount disk cmd:", cmd)
    p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        print("ERROR: failed to mount data_disk device {}", device_name)
        sys.exit(p.returncode)

    return data_disk


def setup_data_disks(cluster_configuration):
    cmd = 'lsblk -lnS --sort name | wc -l'
    p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, _ = p.communicate()
    if int(output) <= 3:
        return

    # by default, there are 3 devices on each host: sda, sdb, sr0
    cmd = 'lsblk -lnbS --sort=name --output NAME,SIZE | grep -v "sr0\|sd[ab]"'
    p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    disks = stdout.decode('UTF-8').split('\n')[:-1]

    disk_size_mapping = {}
    for disk in disks:
        assert len(disk.split()) == 2
        name, size = disk.split()
        # convert size from bytes to gb
        size = int(size) / 1024 / 1024 / 1024
        if not disk_size_mapping.get(size):
            disk_size_mapping[size] = [name]
        else:
            disk_size_mapping[size].append(name)

    for i, defined_data_disk in enumerate(cluster_configuration.data_disks):
        device_name = disk_size_mapping[defined_data_disk.disk_size_gb].pop()
        mounted_data_disk = mount_data_disk(data_disk=defined_data_disk, device_name=device_name, number=i)
        # update cluster_configuration in case mount_path changed
        cluster_configuration[i] = mounted_data_disk
