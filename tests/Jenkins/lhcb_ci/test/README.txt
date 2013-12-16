================================================================================

  README
    lhcb_ci

================================================================================

This test suite includes general tests enforcing conventions and particular tests
going through the details of each implementation. It is divided in two sets of
tests:

  a) lhcb_ci/test/test_{agent,client,db,service}.py
  b) lhcb_ci/test/clients/*
  
The first set is concerned with the configuration and installation of the components.
The second one, can test anything, up to the developer. Please, do not modify tests
in set a unless you really know what are you doing. Note that to keep them clear
from particularities, they do not import any DIRAC or LHCbDIRAC - only use lhcb_ci
code, which is generic enough to accommodate any component. The only exceptions, 
are hardcoded in the modules exceptions.py and links.py.


VM preparation

1) remove any my.cnf file
2) install Java
3) create user lhcbci and set key to access VM from Jenkins plus directory permissions
4) expand VM disk if necessary ( see below )

function resizeDisk(){ 
  echo -e "n\np\n2\n\n\nw" | fdisk /dev/vda
  reboot
}
function createFileSystem(){
  mkfs -t ext3 /dev/vda2
  mkdir /scratch
  mount /dev/vda2 /scratch
  # To make changes permanent
  echo "/dev/vda2 /scratch ext3 defaults 0 2" > /etc/fstab
}