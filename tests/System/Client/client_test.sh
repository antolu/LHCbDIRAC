#!/bin/bash


dir=$( echo "$USER" |cut -c 1)/$USER
echo "this is a test file" >> DMS_Scripts_Test_File.txt

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
echo "======   dirac-admin-site-info LCG.CERN.cern"
dirac-admin-site-info LCG.CERN.cern
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
echo "======  dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw"
dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw
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
echo "====== dirac-dms-lfn-metadata /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw"
dirac-dms-lfn-metadata /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-lfn-accessURL /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw"
dirac-dms-lfn-accessURL /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo " "
echo " ########################## BEGIN OF USER FILES TEST #############################"
echo " "
echo " "
echo "====== dirac-dms-list-directory /lhcb/user/$dir/Dirac_Scripts_Test_Directory/"

echo "======  dirac-dms-storage-usage-summary"
dirac-dms-storage-usage-summary -S CERN-USER -D /lhcb/user/$dir/Dirac_Scripts_Test_Directory/
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-add-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CNAF-USER"
dirac-dms-add-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
mv DMS_Scripts_Test_File.txt DMS_Scripts_Test_File.old
echo "======  dirac-dms-replicate-lfn /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt SARA-USER CNAF-USER"
dirac-dms-replicate-lfn /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt SARA-USER CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-dms-replica-stats /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-replica-stats /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-catalog-metadata /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-catalog-metadata /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-get-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-get-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo "====== dirac-dms-remove-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CNAF-USER"
dirac-dms-remove-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt SARA-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
ls DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
else
   echo "File downloaded properly"
fi
echo " "
echo "====== dirac-dms-remove-files /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CNAF-USER"
dirac-dms-remove-files /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo " "
echo " ########################## END OF USER FILES TEST #############################"
echo " "
echo " "
echo "======  dirac-monitoring-get-components-status"
dirac-monitoring-get-components-status
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
