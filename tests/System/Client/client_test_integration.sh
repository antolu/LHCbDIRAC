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

echo "====== dirac-dms-add-transformation --Visibility=All --BKQuery=/certification/test/Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged/13714010/ALLSTREAMS.DST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start"
#dirac-dms-add-transformation --Visibility=All --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start
dirac-dms-add-transformation --Visibility=All --BKQuery=/certification/test/Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged/13714010/ALLSTREAMS.DST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-replica-stats --Visibility=All --BKQuery=/certification/test/Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged/13714010/ALLSTREAMS.DST"
#dirac-dms-replica-stats  --Visibility=All --BKQuery=/LHCb/Collision12//RealData/Reco13a/Stripping19a//PID.MDST
dirac-dms-replica-stats --Visibility=All --BKQuery=/certification/test/Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged/13714010/ALLSTREAMS.DST
if [ $? -ne 0 ]
then
   exit $?
fi


# FIXME
# It does not make sense to do replication and removal right after each other.
# There needs to be time between.
# Also this file is on Archive only, so can't be read

# echo " "
# echo "===== dirac-dms-create-replication-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst"
# result=`dirac-dms-create-replication-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst`
# if [ $? -ne 0 ]
# then
#    echo $result
#    exit $?
# else
#    echo $result
#    reqID=`echo $result | awk -F ' ' '{print $2}'`
#    echo "===== dirac-rms-request $reqID"
#    dirac-rms-request $reqID
#    if [ $? -ne 0 ]
#    then
#      exit $?
#    fi
# fi

# echo "===== dirac-dms-create-removal-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst"
# dirac-dms-create-removal-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst
# if [ $? -ne 0 ]
# then
#    exit $?
# fi
echo "==== dirac-dms-add-transformation --Visibility=All --BKQuery=/certification/test/Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged/13714010/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test"
dirac-dms-add-transformation --Visibility=All --BKQuery=/certification/test/Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged/13714010/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo " if 2 replicas exists you can run "
echo "====== dirac-dms-add-transformation --Visibility=All --BKQuery=/certification/test/Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged/13714010/ALLSTREAMS.DST --Plugin=ReduceReplicas --NumberOfReplicas=1 --Start ======"
echo " "
echo " if the files are staged you can run"
echo "====== dirac-dms-add-transformation --Plugin=RemoveReplicas --BK=/certification/test/Beam3500GeV-VeloClosed-MagDown/RealData/91000000/RAW  --FromSEs=Tier1-Buffer --Start ======"
