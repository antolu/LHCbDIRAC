#!/bin/bash

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

echo "====== dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start"
#dirac-dms-add-replication --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start
dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-replica-stats --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST"
#dirac-dms-replica-stats  --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST
dirac-dms-replica-stats --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "===== dirac-dms-create-replication-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst"
result=`dirac-dms-create-replication-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst`
if [ $? -ne 0 ]
then
   echo $result
   exit $?
else
   echo $result
   reqID=`echo $result | awk -F ' ' '{print $2}'`
   echo "===== dirac-rms-show-request $reqID"
   dirac-rms-show-request $reqID
   if [ $? -ne 0 ]
   then
     exit $?
   fi
fi

echo "===== dirac-dms-create-removal-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst"
dirac-dms-create-removal-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst
if [ $? -ne 0 ]
then
   exit $?
fi
echo "==== dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test"
dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-bookkeeping-run-files 81789"
dirac-bookkeeping-run-files 81789
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-bookkeeping-gui"
dirac-bookkeeping-gui
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo " if 2 replicas exists you can run "
echo "====== dirac-dms-add-replication --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST --Plugin=DeleteReplicas --NumberOfReplicas=1 --Start
