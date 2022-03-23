#!/bin/bash
NRHUGEPAGES=$1
HUGEPGSZ=`cat /proc/meminfo  | grep Hugepagesize | cut -d : -f 2 | tr -d ' '`

#
# Removes all reserved hugepages.
#
clear_huge_pages()
{
	echo > .echo_tmp
	for d in /sys/devices/system/node/node? ; do
		echo "echo 0 > $d/hugepages/hugepages-${HUGEPGSZ}/nr_hugepages" >> .echo_tmp
	done
	echo "Removing currently reserved hugepages"
	sudo sh .echo_tmp
	rm -f .echo_tmp

	remove_mnt_huge
}

#
# Creates hugepage filesystem.
#
create_mnt_huge()
{
	echo "Creating /mnt/huge and mounting as hugetlbfs"
	sudo mkdir -p /mnt/huge

	grep -s '/mnt/huge' /proc/mounts > /dev/null
	if [ $? -ne 0 ] ; then
		sudo mount -t hugetlbfs nodev /mnt/huge
	fi
}

#
# Removes hugepage filesystem.
#
remove_mnt_huge()
{
	echo "Unmounting /mnt/huge and removing directory"
	grep -s '/mnt/huge' /proc/mounts > /dev/null
	if [ $? -eq 0 ] ; then
		sudo umount /mnt/huge
	fi

	if [ -d /mnt/huge ] ; then
		sudo rm -R /mnt/huge
	fi
}
# clear current huge pages
clear_huge_pages
# reserve new huge pages
echo "echo $NRHUGEPAGES > /sys/kernel/mm/hugepages/hugepages-${HUGEPGSZ}/nr_hugepages" > .echo_tmp
echo "Reserving $NRHUGEPAGES on each node of size ${HUGEPGSZ}"
for d in /sys/devices/system/node/node? ; do
	node=$(basename $d)
	echo "echo $NRHUGEPAGES > $d/hugepages/hugepages-${HUGEPGSZ}/nr_hugepages" >> .echo_tmp
done
sudo sh .echo_tmp
rm -f .echo_tmp

create_mnt_huge


