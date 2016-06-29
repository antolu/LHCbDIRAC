#!/bin/csh -f

echo "lhcb-proxy-init -g lhcb_prmgr"
lhcb-proxy-init -g lhcb_shifter
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-proxy-info"
dirac-proxy-info
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-configuration-dump-local-cache"
dirac-configuration-dump-local-cache
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-admin-get-banned-sites"
dirac-admin-get-banned-sites
if $status != 0 then
   exit 0
endif
echo " "
echo "======   dirac-admin-site-info LCG.CERN.ch"
dirac-admin-site-info LCG.CERN.ch
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-dms-show-se-status"
dirac-dms-show-se-status
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-dms-storage-usage-summary"
dirac-dms-storage-usage-summary -S CERN-USER -D /lhcb/
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw"
dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-dms-replicate-lfn /lhcb/user/m/msoares/hlta0102.txt CNAF-USER"
dirac-dms-replicate-lfn /lhcb/user/m/msoares/hlta0102.txt CNAF-USER
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-dms-check-directory-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789"
dirac-dms-check-directory-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789
if $status != 0 then
   exit 0
endif
echo " "
echo "====== dirac-dms-check-file-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw"
dirac-dms-check-file-integrity /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-monitoring-get-components-status"
dirac-monitoring-get-components-status
if $status != 0 then
   exit 0
endif
echo " "
echo "====== dirac-dms-remove-replicas /lhcb/user/m/msoares/hlta0102.txt CNAF-USER"
dirac-dms-remove-replicas /lhcb/user/m/msoares/hlta0102.txt CNAF-USER
if $status != 0 then
   exit 0
endif
echo " "
echo "====== dirac-dms-remove-files /lhcb/user/m/msoares/hlta0102.txt CNAF-USER"
dirac-dms-remove-files /lhcb/user/m/msoares/hlta0102.txt CNAF-USER
if $status != 0 then
   exit 0
endif
echo " "

