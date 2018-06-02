#!/bin/bash
devicename=$1
parted --script --align optimal ${devicename} mklabel gpt
parted --script --align optimal ${devicename} mkpart primary ext4 0% 100%
mkfs.ext4 ${devicename}1
mkdir -p /mnt/data-disk1
echo "${devicename}1       /mnt/data-disk1	auto    defaults,nofail        0 0" >> /etc/fstab
mount ${devicename}1 /mnt/data-disk1
