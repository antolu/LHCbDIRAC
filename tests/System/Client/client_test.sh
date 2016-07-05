#!/bin/bash			


dir=$( echo "$USER" |cut -c 1)/$USER
ls>>hlta0102.txt

echo "lhcb-proxy-init -g lhcb_prmgr"
lhcb-proxy-init -g lhcb_prmgr
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-proxy-info"
dirac-proxy-info
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-configuration-dump-local-cache"
dirac-configuration-dump-local-cache
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-admin-get-banned-sites"
dirac-admin-get-banned-sites
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======   dirac-admin-site-info LCG.CERN.ch"
dirac-admin-site-info LCG.CERN.ch
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-dms-show-se-status"
dirac-dms-show-se-status
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-dms-storage-usage-summary"
dirac-dms-storage-usage-summary -S CERN-USER -D /lhcb/
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw"
dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-add-file /lhcb/user/$dir/hlta0102.txt CNAF-USER"
dirac-dms-add-file /lhcb/user/$dir/hlta0102.txt ./hlta0102.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo "======  dirac-dms-replicate-lfn /lhcb/user/$dir/hlta0102.txt CNAF-USER"
dirac-dms-replicate-lfn /lhcb/user/$dir/hlta0102.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-dms-check-directory-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789"
dirac-dms-check-directory-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-check-file-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw"
dirac-dms-check-file-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-monitoring-get-components-status"
dirac-monitoring-get-components-status
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-remove-replicas /lhcb/user/$dir/hlta0102.txt CNAF-USER"
dirac-dms-remove-replicas /lhcb/user/$dir/hlta0102.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-remove-files /lhcb/user/$dir/hlta0102.txt CNAF-USER"
dirac-dms-remove-files /lhcb/user/$dir/hlta0102.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

