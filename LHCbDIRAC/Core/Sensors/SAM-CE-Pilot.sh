#!/bin/bash
source /afs/cern.ch/project/gd/LCG-share/sl4/etc/profile.d/grid-env.sh
cat /afs/cern.ch/user/s/santinel/private/pas |voms-proxy-init --voms lhcb:/lhcb/Role=lcgadmin  -pwstdin -out ~santinel/sgm_proxy
export SAMHOME=/afs/cern.ch/user/s/santinel/scratch0/grid/same/client
export TOPDIR=/afs/cern.ch/user/s/santinel/scratch0/grid/pilot_mapping_test
export X509_USER_PROXY=~santinel/sgm_proxy


for i in `/afs/cern.ch/user/s/santinel/scratch0/grid/same/client/bin/same-query nodename serviceabbr=CE servicestatusvo=lhcb voname=lhcb | sort`;do
	$TOPDIR/CE-lhcb-pilot.sh $i > $TOPDIR/result/$i 2>&1	
done

export dir_result=$TOPDIR/result

for i in `ls $TOPDIR/result`;do 
	if [ ! -d $TOPDIR/sam/$i ];then
	 mkdir $TOPDIR/sam/$i
	fi	
	 export nodename=`cat $dir_result/$i |grep nodename|cut -d "=" -f2`
	 export samresult=`cat $dir_result/$i |grep SAMOUT|cut -d "=" -f2`	
         echo "testName: CE-lhcb-pilot" > $TOPDIR/sam/$i/CE-lhcb-pilot.def
         echo "testAbbr: PilotRole" >> $TOPDIR/sam/$i/CE-lhcb-pilot.def
         echo "testTitle: LHCb VOMS pilot role verification" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.def
         echo "testHelp: https://santinel.web.cern.ch/santinel/SAM/CE-Pilot-deploy" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.def
         echo "EOT" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.def
         echo "envName: CE-${i}.$$" >  $TOPDIR/sam/$i/CE-lhcb-pilot.env
         echo "name: dummy" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.env
         echo "value: none" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.env
         echo "nodename: $nodename" >  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "testname: CE-lhcb-pilot" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "envName: CE-${i}.$$" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "voname: lhcb" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "status: $samresult" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "detaileddata: EOT" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "<br>" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         sed 's/$/<br>/g' $dir_result/$i >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "<br>" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
         echo "EOT" >>  $TOPDIR/sam/$i/CE-lhcb-pilot.result
	 rm $dir_result/$i
         $SAMHOME/bin/same-publish-tuples  TestDef  $TOPDIR/sam/$i/CE-lhcb-pilot.def
         $SAMHOME/bin/same-publish-tuples TestEnvVars $TOPDIR/sam/$i/CE-lhcb-pilot.env
         $SAMHOME/bin/same-publish-tuples TestData $TOPDIR/sam/$i/CE-lhcb-pilot.result
	 rm $TOPDIR/sam/$i/*
done

rm ~santinel/sgm_proxy
