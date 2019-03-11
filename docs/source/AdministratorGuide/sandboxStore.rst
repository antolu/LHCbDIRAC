=============
Sandbox Store
=============

Our Sandbox store is hosted on a VM (currently lbvobox110) and the files are stored on an external volume (currently mounted under `/opt/dirac/storage`)


How To resize the sandbox store volume
======================================

Resize the sandbox volume since it is full. The normal procedure can be found here:
https://clouddocs.web.cern.ch/clouddocs/details/working_with_volumes.html
However, for historical reasons, we do not have a filesystem directly on the volume, but we have LVM. So the
procedure goes as follow:

* stop all the services using the volume (SandboxStore, but potentially other system services)
* unmount the FS from lbvobox110
* Deactivate the logical volume
* unmount + resize the volume as per the doc
* tell lbvobox110 lvm to forget about what it knows
* remount the volume
* tell lvm to scan what it finds, resize the pv, and activate it
* resize the logical volume
* mount the file system
* resize the file system

I could/should have resized before mounting, it might have been faster. With the procedure bellow, expect a 1h resize time for 500GB.

Step by step::

  # Unmount the FS
  [root@lbvobox110 ~]# umount /opt/dirac/storage/


  # Find the volume, detach it, and extend it
  [chaen@lxplus097 ~]$ openstack volume list | grep sandbox
  | 8ab0da8d-477b-459b-a4e4-5fd9c57f1241 | dirac-sandbox                | in-use | 1500 | Attached to lbvobox110
  on /dev/vdc    |

  [chaen@lxplus097 ~]$ nova volume-detach lbvobox110 8ab0da8d-477b-459b-a4e4-5fd9c57f1241
  [chaen@lxplus097 ~]$ cinder extend 8ab0da8d-477b-459b-a4e4-5fd9c57f1241 3000


  # Deactivate the volume (CAUTION: this step was found to be missing, and was added without being tested)
  [root@lbvobox110 ~]# lvchange -a n /dev/VolGroup02/sandbox

  # Tell lvm to forget everything
  [root@lbvobox110 ~]# dmsetup remove /dev/VolGroup02/*


  # reattach the volume
  [chaen@lxplus097 ~]$ nova volume-attach lbvobox110 8ab0da8d-477b-459b-a4e4-5fd9c57f1241
  +----------+--------------------------------------+
  | Property | Value                                |
  +----------+--------------------------------------+
  | device   | /dev/vdc                             |
  | id       | 8ab0da8d-477b-459b-a4e4-5fd9c57f1241 |
  | serverId | a36daa82-3b6c-4018-b56d-fa5cfa376a76 |
  | volumeId | 8ab0da8d-477b-459b-a4e4-5fd9c57f1241 |
  +----------+--------------------------------------+


  # Scan the physical volume, resize it, and extend the logical volume
  [root@lbvobox110 ~]# pvscan

  # NOTE: last time we did this procedure, the scan did not give us the size we expected, so we
  # had to manually resize it. Not clear why. In case it does not have the size you want, do the following
  [root@lbvobox110 ~]# pvresize /dev/vdc

  [root@lbvobox110 ~]# lvextend -L2.92T /dev/VolGroup02/sandbox
    Rounding size to boundary between physical extents: 2.92 TiB.
    Size of logical volume VolGroup02/sandbox changed from 1.42 TiB (371200 extents) to 2.92 TiB (765461 extents).
    Logical volume sandbox successfully resized.

  # Activate it
  [root@lbvobox110 ~]# lvchange -a y /dev/VolGroup02/sandbox

  # Mount the file system
  [root@lbvobox110 ~]# mount /dev/VolGroup02/sandbox /opt/dirac/storage

  # resize it
  [root@lbvobox110 ~]# resize2fs /dev/VolGroup02/sandbox
  resize2fs 1.41.12 (17-May-2010)
  Filesystem at /dev/VolGroup02/sandbox is mounted on /opt/dirac/storage; on-line resizing required
  old desc_blocks = 91, new_desc_blocks = 187
  Performing an on-line resize of /dev/VolGroup02/sandbox to 783832064 (4k) blocks.