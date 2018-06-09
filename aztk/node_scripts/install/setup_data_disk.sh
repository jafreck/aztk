#!/bin/bash

set -e

devicename="/dev/"$1
format_type=$2
mount_path=$3
device_partition_name="${devicename}1"

# make parition
parted --script --align optimal ${devicename} mklabel gpt
parted --script --align optimal ${devicename} mkpart primary ext4 0% 100%

# format partition
sleep 1
mkfs.${format_type} ${device_partition_name}

# make partition directory
mkdir -p ${mount_path}

# auto mount parition on reboot
echo "${device_partition_name}       ${mount_path}	auto    defaults,nofail        0 0" >> /etc/fstab

# mount partition
mount ${device_partition_name} ${mount_path}
