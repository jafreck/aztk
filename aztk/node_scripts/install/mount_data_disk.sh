#!/bin/bash
devicename=$1
datadisknumber=$2

device_partition_name="${devicename}1"
data_disk_mount_point="/data-disk${datadisknumber}"

# make parition
parted --script --align optimal ${devicename} mklabel gpt
parted --script --align optimal ${devicename} mkpart primary ext4 0% 100%

# format partition
mkfs.ext4 ${device_partition_name}

# make partition directory
mkdir -p ${data_disk_mount_point}

# auto mount parition on reboot
echo "${device_partition_name}       ${data_disk_mount_point}	auto    defaults,nofail        0 0" >> /etc/fstab

# mount partition
mount ${device_partition_name} ${data_disk_mount_point}
