#!/bin/csh -f

echo "lhcb-proxy-init -g lhcb_prod"
lhcb-proxy-init -g lhcb_prod
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
echo "======  dirac-dms-replicate-lfn /lhcb/user/j/joel/hlta0102.txt CNAF-USER"
dirac-dms-replicate-lfn /lhcb/user/j/joel/hlta0102.txt CNAF-USER
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
echo "====== dirac-dms-show-fts-status"
dirac-dms-show-fts-status
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-wms-get-normalized-queue-length lcgce02.gridpp.rl.ac.uk:8443/cream-pbs-grid3000M"
dirac-wms-get-normalized-queue-length lcgce02.gridpp.rl.ac.uk:8443/cream-pbs-grid3000M
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
echo "====== dirac-dms-remove-replicas /lhcb/user/j/joel/hlta0102.txt CNAF-USER"
dirac-dms-remove-replicas /lhcb/user/j/joel/hlta0102.txt CNAF-USER
if $status != 0 then
   exit 0
endif
echo " "
echo "====== dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test"
#dirac-dms-add-replication --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST --Plugin=ReplicateDataset --Test
dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test
if $status != 0 then
   exit 0
endif
echo " "
echo "====== dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start"
#dirac-dms-add-replication --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start
dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start
if $status != 0 then
   exit 0
endif
echo " "
echo "====== dirac-dms-replica-stats --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST"
#dirac-dms-replica-stats  --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST
dirac-dms-replica-stats --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST
if $status != 0 then
   exit 0
endif
echo " "
echo "===== dirac-dms-create-replication-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst"
set result=`dirac-dms-create-replication-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst`
if $status != 0 then
   echo $result
   exit 0
else
   echo $result
   set reqID=`echo $result | awk -F ' ' '{print $2}'`
   echo "===== dirac-rms-show-request $reqID"
   dirac-rms-show-request $reqID
   if $status != 0 then
     exit 0
   endif   
endif

echo "===== dirac-dms-create-removal-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst"
dirac-dms-create-removal-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst
if $status != 0 then
   exit 0
endif

echo "==== dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test"
dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test
if $status != 0 then
   exit 0
endif


echo " "
echo "======  dirac-bookkeeping-run-files 81789"
dirac-bookkeeping-run-files 81789
if $status != 0 then
   exit 0
endif
echo " "
echo "======  dirac-bookkeeping-gui"
dirac-bookkeeping-gui
if $status != 0 then
   exit 0
endif
exit 0
echo " "
echo " if 2 replicas exists you can run "
echo "====== dirac-dms-add-replication --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST --Plugin=DeleteReplicas --NumberOfReplicas=1 --Start"
