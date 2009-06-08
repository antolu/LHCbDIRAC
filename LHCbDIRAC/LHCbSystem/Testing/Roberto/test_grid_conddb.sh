#!/bin/sh 

source $VO_LHCB_SW_DIR/lib/scripts/SetupProject.sh
export MYSITEROOT=$VO_LHCB_SW_DIR/lib

#if [ x"$LFC_HOST" == "x" ]; then
    export LFC_HOST=lfc-lhcb-ro.cern.ch
#fi

echo "Running CondDB access test on node `hostname`" > SAMOUT



edg-brokerinfo getCE > /dev/null 2>&1

if [ "$?" == "0" ]; then
    export HOST_CE=`edg-brokerinfo getCE | cut -d: -f1 2 >>SAMOUT`
else
    export HOST_CE=`glite-brokerinfo getCE | cut -d: -f1 2>>SAMOUT`
fi

if [ x"$HOST_CE" == "x" ]; then
    export HOST_CE=`echo $GLOBUS_CE | cut -d : -f 1`
fi

echo "NODENAME:$HOST_CE" >> SAMOUT

if [ -z "$1" ]; then
    data=DC06
else
    data=$1
    shift
fi

if [ -z "$1" ]; then
    export DIRACSITE=LCG.CERN.ch
else
    export DIRACSITE=$1
    shift
fi

echo "DIRACSITE set to " $DIRACSITE >>SAMOUT 
echo "data set to " $data >>SAMOUT

export CMTCONFIG=slc4_ia32_gcc34

. $VO_LHCB_SW_DIR/lib/scripts/ExtCMT.sh >> SAMOUT 2>&1
#. /afs/cern.ch/lhcb/scripts/CMT.sh >> SAMOUT 2>&1

SetupProject LHCb v25r2 >> SAMOUT 2>&1

#SetupProject LHCb >> SAMOUT 2>&1

gaudirun.py -v LoadDDDB_$data.py >> SAMOUT 2>&1

gaudi_code=$?

error=0
fatal=0

export error=`grep ERROR SAMOUT |wc -l`;


if [ "$error" != "0" ]; then
    echo "SAMRESULT:50" >>SAMOUT
    exit $SAME_ERROR
else
 if [ "$gaudi_code" != "0" ];then
    echo "non zero return code from gaudirun.py"  >>SAMOUT
    echo "SAMRESULT:50" >>SAMOUT
    exit $SAME_ERROR
 fi
fi

export fatal=`grep FATAL SAMOUT |wc -l`;

if [ "$fatal" != "0" ]; then
    echo "SAMRESULT:60" >>SAMOUT
    exit $SAME_CRITICAL
fi

export warning=`grep WARNING SAMOUT |wc -l`;

echo "SAMRESULT:10" >>SAMOUT  
exit $SAME_OK
