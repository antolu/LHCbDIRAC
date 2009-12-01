#!/bin/bash

source /afs/cern.ch/project/gd/LCG-share/sl4/etc/profile.d/grid-env.sh
cat /afs/cern.ch/user/s/santinel/private/pas |voms-proxy-init --voms lhcb:/lhcb/Role=production -pwstdin

TOPDIR=/afs/cern.ch/user/s/santinel/scratch0/grid/conditionDB
SAMHOME=/afs/cern.ch/user/s/santinel/scratch0/grid/same/client

if [ ! -d $TOPDIR/jdl ];then
  mkdir $TOPDIR/jdl
fi 

if [ ! -d $TOPDIR/guids ];then
   mkdir $TOPDIR/guids
fi 

if [ ! -d $TOPDIR/results ];then
  mkdir $TOPDIR/results
fi 
if [ ! -d $TOPDIR/sam ];then
  mkdir $TOPDIR/sam
fi


#removing stale jobs 
for i in `find $TOPDIR/guids -mtime +2`;do
 rm $i
done


for i in `ls $TOPDIR/sites`;do
 done=0
 if [ -f $TOPDIR/guids/$i ]; then
     export prop=`cat $TOPDIR/guids/$i |wc -l`
     if [ "$prop" == "2" ]; then 
	 export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
     else
	 rm $TOPDIR/guids/$i
     fi
 fi   
 #done can be 0 if job is not complete or if no guid found or not properly formatted

 if [ "$done" == "1" ];then
     export dir_result=`glite-wms-job-output --dir $TOPDIR/results/$i -i $TOPDIR/guids/$i | grep $TOPDIR`
     if [ "$?" == "0" ];then
	 export nodename=`cat $dir_result/SAMOUT |grep NODENAME|cut -d ":" -f2`
	 export samresult=`cat $dir_result/SAMOUT |grep SAMRESULT|cut -d ":" -f2`
	 echo "testName: CE-lhcb-CondDB" > $TOPDIR/sam/$i/CE-lhcb-CondDB.def
	 echo "testAbbr: ConditionDB" >> $TOPDIR/sam/$i/CE-lhcb-CondDB.def
	 echo "testTitle: LHCb ConditionDB Access" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.def
	 echo "testHelp: https://santinel.web.cern.ch/santinel/SAM/SRM-lhcb-AccessFile" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.def
	 echo "EOT" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.def
	 echo "envName: CE-${i}.$$" >  $TOPDIR/sam/$i/CE-lhcb-CondDB.env
	 echo "name: dummy" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.env
	 echo "value: none" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.env
	 echo "nodename: $nodename" >  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "testname: CE-lhcb-CondDB" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "envName: CE-${i}.$$" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "voname: lhcb" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "status: $samresult" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "detaileddata: EOT" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "<br>" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 sed 's/$/<br>/g' $dir_result/SAMOUT >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "<br>" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 echo "EOT" >>  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 $SAMHOME/bin/same-publish-tuples  TestDef  $TOPDIR/sam/$i/CE-lhcb-CondDB.def
	 $SAMHOME/bin/same-publish-tuples TestEnvVars  $TOPDIR/sam/$i/CE-lhcb-CondDB.env
	 $SAMHOME/bin/same-publish-tuples TestData  $TOPDIR/sam/$i/CE-lhcb-CondDB.result
	 rm $TOPDIR/guids/$i
	 rm -rf $TOPDIR/results/$i
     fi
 fi
done

for i in `ls $TOPDIR/sites`;do
    if [ "$i" == "cern" ]; then
	if [ -f $TOPDIR/guids/$i ]; then
	    export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
	    export failed=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Aborted|wc -l`;
	    export cleared=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Cleared|wc -l`;
	fi
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
	    echo "Executable    = \"test_grid_conddb.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/test_grid_conddb.sh\",\"$TOPDIR/LoadDDDB_DC06.py\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "Arguments = \"DC06 LCG.CERN.ch\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"cern.ch\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;
	fi

    fi
    
    if [ "$i" == "cnaf" ]; then
	if [ -f $TOPDIR/guids/$i ]; then
	    export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
	    export failed=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Aborted|wc -l`;
	    export cleared=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Cleared|wc -l`;
	fi
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
	    echo "Executable    = \"test_grid_conddb.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/test_grid_conddb.sh\",\"$TOPDIR/LoadDDDB_DC06.py\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "Arguments = \"DC06 LCG.CNAF.it\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"ce04-lcg.cr.cnaf.infn.it\",other.GlueCEUniqueId) )||  ( Regexp(\"ce05-lcg.cr.cnaf.infn.it\",other.GlueCEUniqueId) )|| ( Regexp(\"ce06-lcg.cr.cnaf.infn.it\",other.GlueCEUniqueId) )) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
#	    echo "Requirements = ( ( Regexp(\"cr.cnaf.infn.it\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;
	fi
    fi
    
    
    if [ "$i" == "ral" ]; then
	if [ -f $TOPDIR/guids/$i ]; then
	    export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
	    export failed=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Aborted|wc -l`;
	    export cleared=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Cleared|wc -l`;
	fi
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
	    echo "Executable    = \"test_grid_conddb.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/test_grid_conddb.sh\",\"$TOPDIR/LoadDDDB_DC06.py\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "Arguments = \"DC06 LCG.RAL.uk\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"gridpp.rl.ac.uk\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 


	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;
	fi
    fi
    if [ "$i" == "pic" ]; then
	if [ -f $TOPDIR/guids/$i ]; then
	    export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
	    export failed=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Aborted|wc -l`;
	    export cleared=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Cleared|wc -l`;
	fi
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
	    echo "Executable    = \"test_grid_conddb.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/test_grid_conddb.sh\",\"$TOPDIR/LoadDDDB_DC06.py\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "Arguments = \"DC06 LCG.PIC.es\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"pic.es\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;
	fi
    fi
    if [ "$i" == "sara" ]; then
	if [ -f $TOPDIR/guids/$i ]; then
	    export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
	    export failed=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Aborted|wc -l`;
	    export cleared=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Cleared|wc -l`;
	fi
	#if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
	    echo "Executable    = \"test_grid_conddb.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/test_grid_conddb.sh\",\"$TOPDIR/LoadDDDB_DC06.py\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "Arguments = \"DC06 LCG.NIKHEF.nl\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"nikhef.nl\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;
	fi
    fi
    if [ "$i" == "in2p3" ]; then
	if [ -f $TOPDIR/guids/$i ]; then
	    export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
	    export failed=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Aborted|wc -l`;
	    export cleared=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Cleared|wc -l`;
	fi
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	    echo "Executable    = \"test_grid_conddb.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/test_grid_conddb.sh\",\"$TOPDIR/LoadDDDB_DC06.py\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "Arguments = \"DC06 LCG.IN2P3.fr\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"cclcgceli03.in2p3.fr\",other.GlueCEUniqueId) )|| ( Regexp(\"cclcgceli04.in2p3.fr\",other.GlueCEUniqueId) )) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;

	fi
    fi
    if [ "$i" == "gridka" ]; then
	if [ -f $TOPDIR/guids/$i ]; then
	    export done=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Done|wc -l`;
	    export failed=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Aborted|wc -l`;
	    export cleared=`glite-wms-job-status -i $TOPDIR/guids/$i |grep Cleared|wc -l`;
	fi
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	    echo "Executable    = \"test_grid_conddb.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/test_grid_conddb.sh\",\"$TOPDIR/LoadDDDB_DC06.py\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "Arguments = \"DC06 LCG.GRIDKA.de\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;	    
	    echo "Requirements = ( ( Regexp(\"gridka.de\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;
	fi
    fi


done

