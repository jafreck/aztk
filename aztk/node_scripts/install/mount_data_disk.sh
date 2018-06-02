#!/bin/bash
devicename=$1
parted --script --align optimal /dev/sdd mklabel gpt
parted --script --align optimal /dev/sdd mkpart primary ext4 0% 100%
mkfs.ext4 /dev/sdd1
mkdir -p /mnt/data-disk1
echo "/dev/sdd1       /mnt/data-disk1	auto    defaults,nofail        0 0" >> /etc/fstab
mount /dev/sdd1 /mnt/data-disk1
