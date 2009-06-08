#!/bin/csh

setenv TOPDIR /afs/cern.ch/user/s/santinel/scratch0/grid/unit_test_srm2
setenv SAMHOME /afs/cern.ch/user/s/santinel/scratch0/grid/same/client
setenv ST $1
#source ~/.diracsh
#source /afs/cern.ch/lhcb/scripts/lhcbsetup.sh
#source /afs/cern.ch/lhcb/scripts/CMT.sh

source /afs/cern.ch/lhcb/software/releases/LBSCRIPTS/LBSCRIPTS_v2r2/InstallArea/scripts/LbLogin.csh

SetupProject Dirac v4r8
#SetupProject --dev Dirac 
if ("x$1" == "x") then 
    echo "Error: no Space token defined"
    exit 1
endif

cat /afs/cern.ch/user/s/santinel/private/pas |lhcb-proxy-init -g lhcb_prod --pwstdin

foreach i (CERN CNAF PIC GRIDKA IN2P3 RAL NIKHEF)
    echo "testing $i-$1 ...."
    cd $TOPDIR/tmp
    rm $TOPDIR/$1/result/$i
    #python2.4 /afs/cern.ch/user/s/santinel/public/sam/TestStoragePlugIn.py $i-$1 -o LogLevel=ERROR >& $TOPDIR/$1/result/$i
    python /afs/cern.ch/user/s/santinel/scratch0/grid/lhcb/DIRAC/DIRAC3/DIRAC/DataManagementSystem/Client/test/TestStoragePlugIn.py $i-$1 -o LogLevel=ERROR >& $TOPDIR/$1/result/$i
end


foreach i (CERN CNAF PIC GRIDKA IN2P3 RAL NIKHEF)
    echo "now analysing results for Space token $1 for storage $i"
    rm $TOPDIR/$1/sam/$i/*
    cd $TOPDIR/$1/result
    set nodename=`grep 'destDir=' $TOPDIR/$1/result/$i | cut -d ":" -f 2 | sed -e 's/\///g'`
    echo $nodename
    if ("x$nodename" == "x") then
	if ("$i" == "NIKHEF") then
	    setenv nodename "srm.grid.sara.nl"
	endif

	if ("$i" == "CERN") then
	    setenv nodename "srm-lhcb.cern.ch"
	endif

	if ("$i" == "RAL") then
	    setenv nodename "srm-lhcb.gridpp.rl.ac.uk"
	endif

	if ("$i" == "PIC") then
	    setenv nodename "srmlhcb.pic.es"
	endif

	if ("$i" == "GRIDKA") then
	    setenv nodename "gridka-dCache.fzk.de"
	endif

	if ("$i" == "IN2P3") then
	    setenv nodename "ccsrm.in2p3.fr"
	endif

	if ("$i" == "CNAF") then
	    if ("${1}" == "RAW") then
		setenv nodename "srm-v2.cr.cnaf.infn.it"
	    else
		setenv nodename "storm-fe-lhcb.cr.cnaf.infn.it"
	    endif
	endif
    endif

rm $TOPDIR/${1}/result/$i.h
set result=`grep FAILED $TOPDIR/$1/result/$i|wc -l`
    

if ("$result" ==  "1" ) then
   echo "********Test for $nodename is not OK********" > $TOPDIR/${1}/result/$i.h
   echo "" >> $TOPDIR/${1}/result/$i.h
   setenv samresult 50 
   grep FAILED  $TOPDIR/$1/result/$i >> $TOPDIR/${1}/result/$i.h
   grep FAIL:  $TOPDIR/$1/result/$i >> $TOPDIR/${1}/result/$i.h
   echo " " >> $TOPDIR/${1}/result/$i.h
   echo "Please look in the output for these tests failing causes" >> $TOPDIR/${1}/result/$i.h
   echo " " >> $TOPDIR/${1}/result/$i.h
   echo "****************************************" >> $TOPDIR/${1}/result/$i.h
   echo "" >> $TOPDIR/${1}/result/$i.h
else    
   echo "********Test for $nodename is OK********" > $TOPDIR/${1}/result/$i.h
   setenv samresult 10 
   echo "" >> $TOPDIR/${1}/result/$i.h
   echo "****************************************" >> $TOPDIR/${1}/result/$i.h
   echo "" >> $TOPDIR/${1}/result/$i.h
endif

    echo "testName: SRMv2-lhcb-DiracUnitTest$1" > $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.def
    echo "testAbbr: DiracTest$1" >> $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest$1.def
    echo "testTitle: LHCb Full Unit Test against $1 ST" >>  $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.def
    echo "testHelp: https://santinel.web.cern.ch/santinel/SAM/SRM-lhcb-UnitTest" >>  $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.def
    echo "EOT" >>  $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.def
    echo "envName: SRMv2-${i}.$$" >  $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.env
    echo "name: dummy" >>  $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.env
    echo "value: none" >>  $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.env
    echo "nodename: $nodename" >  $TOPDIR/$1/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "testname: SRMv2-lhcb-DiracUnitTest$1" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "envName: SRMv2-${i}.$$" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "voname: lhcb" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "status: $samresult" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "detaileddata: EOT" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "<br>" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    sed 's/$/<br>/g' $TOPDIR/${1}/result/$i.h >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    sed 's/$/<br>/g' $TOPDIR/${1}/result/$i >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "<br>" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    echo "EOT" >>  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
    $SAMHOME/bin/same-publish-tuples  TestDef  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.def
    $SAMHOME/bin/same-publish-tuples TestEnvVars  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.env
    $SAMHOME/bin/same-publish-tuples TestData  $TOPDIR/${1}/sam/$i/SRMv2-lhcb-DiracUnitTest${1}.result
end

