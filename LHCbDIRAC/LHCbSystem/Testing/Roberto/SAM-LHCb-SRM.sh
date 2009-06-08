#!/bin/bash

source /afs/cern.ch/project/gd/LCG-share/sl4/etc/profile.d/grid-env.sh
cat /afs/cern.ch/user/s/santinel/private/pas |voms-proxy-init --voms lhcb:/lhcb/Role=production -pwstdin

TOPDIR=/afs/cern.ch/user/s/santinel/scratch0/grid/test_rfio
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

# if [ -f $TOPDIR/guids/$i ]; then
#	    export done=`glite-wms-job-status -i$TOPDIR/guids/$i |grep Done |wc -l`;
# fi   
 
 if [ "$done" == "1" ];then
     export dir_result=`glite-wms-job-output --dir $TOPDIR/results/$i -i $TOPDIR/guids/$i | grep $TOPDIR`
     if [ "$?" == "0" ];then
	 export nodename=`cat $dir_result/SAMOUT |grep NODENAME|cut -d ":" -f2`
	 export samresult=`cat $dir_result/SAMOUT |grep SAMRESULT|cut -d ":" -f2`
	 echo "testName: SRMv2-lhcb-FileAccess" > $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.def
	 echo "testAbbr: FileAccessV2" >> $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.def
	 echo "testTitle: SRM(v2) LHCb File Access" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.def
	 echo "testHelp: https://santinel.web.cern.ch/santinel/SAM/SRM-lhcb-AccessFile" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.def
	 echo "EOT" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.def
	 echo "envName: SRMv2-${i}.$$" >  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.env
	 echo "name: dummy" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.env
	 echo "value: none" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.env
	 echo "nodename: $nodename" >  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "testname: SRMv2-lhcb-FileAccess" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "envName: SRMv2-${i}.$$" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "voname: lhcb" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "status: $samresult" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "detaileddata: EOT" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "<br>" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 sed 's/$/<br>/g' $dir_result/SAMOUT >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
#	 cat $dir_result/SAMOUT >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "<br>" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 echo "EOT" >>  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
	 $SAMHOME/bin/same-publish-tuples  TestDef  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.def
	 $SAMHOME/bin/same-publish-tuples TestEnvVars  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.env
	 $SAMHOME/bin/same-publish-tuples TestData  $TOPDIR/sam/$i/SRMv2-lhcb-FileAccess.result
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
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
	    echo "Executable    = \"SRM-lhcb-AccessFile.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/SRM-lhcb-AccessFile.sh\"};">> $TOPDIR/jdl/$i.jdl; 
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
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
	    echo "Executable    = \"SRM-lhcb-AccessFile.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/SRM-lhcb-AccessFile.sh\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"cr.cnaf.infn.it\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
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
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	    echo "Executable    = \"SRM-lhcb-AccessFile.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/SRM-lhcb-AccessFile.sh\"};">> $TOPDIR/jdl/$i.jdl; 
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
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	    echo "Executable    = \"SRM-lhcb-AccessFile.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/SRM-lhcb-AccessFile.sh\"};">> $TOPDIR/jdl/$i.jdl; 
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
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]; then
	    echo "Executable    = \"SRM-lhcb-AccessFile.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/SRM-lhcb-AccessFile.sh\"};">> $TOPDIR/jdl/$i.jdl; 
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
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
	    echo "Executable    = \"SRM-lhcb-AccessFile.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/SRM-lhcb-AccessFile.sh\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"cclcgceli03.in2p3.fr\",other.GlueCEUniqueId) )|| ( Regexp(\"cclcgceli04.in2p3.fr\",other.GlueCEUniqueId) )) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl;
#	    echo "Requirements = ( ( Regexp(\"cclcgceli03.in2p3.fr\",other.GlueCEUniqueId) )|| ( Regexp(\"cclcgceli04.in2p3.fr\",other.GlueCEUniqueId) ) || ( Regexp(\"cclcgceli05.in2p3.fr\",other.GlueCEUniqueId) )|| ( Regexp(\"cclcgceli06.in2p3.fr\",other.GlueCEUniqueId) )) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl;

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
#	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]; then
	if [ "$done" == "1" ]||[ ! -f $TOPDIR/guids/$i ]||[ "$failed" == "1" ]||[ "$cleared" == "1" ]; then
	    echo "Executable    = \"SRM-lhcb-AccessFile.sh\";"> $TOPDIR/jdl/$i.jdl;
	    echo "StdOutput     = \"std.out\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "StdError    =\"std.err\";">> $TOPDIR/jdl/$i.jdl; 
	    echo "InputSandbox  = {\"$TOPDIR/SRM-lhcb-AccessFile.sh\"};">> $TOPDIR/jdl/$i.jdl; 
	    echo "OutputSandbox = {\"std.out\",\"std.err\",\"SAMOUT\"};">> $TOPDIR/jdl/$i.jdl;
	    echo "Requirements = ( ( Regexp(\"gridka.de\",other.GlueCEUniqueId) ) ) && ( other.GlueCEStateStatus == \"Production\" );">>$TOPDIR/jdl/$i.jdl; 
	    rm $TOPDIR/guids/$i
	    glite-wms-job-submit -a -o $TOPDIR/guids/$i $TOPDIR/jdl/$i.jdl;
	fi
    fi


done

