#!/bin/bash 
 
##########################################################################################################
# 
# SRM-lhcb-AccessFile: Author R.Santinelli 20/02/2008
#
# SAM SRMv2 sensor's test for LHCb
# 
# The test does: 
# 
# 1. Retrieves from .brokerinfo the region the job landed on
# 2. For each region retrieves the RDST test SURL and the supported protocol
# 3. Retrieves the TURL using SURL and protocol for that region via SRMv2 interface
# 4. Installs and configures  root application 
# 5. Builds a ROOT macro and runs it by opening the turl returned by SRMv2
# 6. Publishes the SAM results (generates from scratch necessary SAM files and run SAM client from the WN)
#
##########################################################################################################


country_code=""
TURL=""
SURL=""
HOST_CE=""
export protocols="dcap gsidcap file root rfio"


echo "Running file access test on node $HOST_CE" > SAMOUT
hostname >>SAMOUT
edg-brokerinfo getCE > /dev/null 2>&1
if [ "$?" == "0" ];then 
    export HOST_CE=`edg-brokerinfo getCE | cut -d: -f1 2 >>SAMOUT`
else
    export HOST_CE=`glite-brokerinfo getCE | cut -d: -f1 2>>SAMOUT` 
fi

if [ x"$HOST_CE" == "x" ];then
    export HOST_CE=`hostname`
fi

for i in `echo $HOST_CE | tr '.' ' '`;do
    var=${i}
done


country_code=$var

echo "Country code is $country_code" >>SAMOUT

if [ "$country_code" == "" ];then
  echo "No surl found or domain found from brokerinfo" >>SAMOUT
  echo "SAMRESULT:40" >>SAMOUT
  exit $SAME_WARNING
fi

case $country_code in
    ch)    export SURL="srm://srm-lhcb.cern.ch:8443/castor/cern.ch/grid/lhcb/test/SAM/zz_zz.dst" ; export nodename="srm-lhcb.cern.ch"   ;;
    it)    export SURL="srm://storm-fe-lhcb.cr.cnaf.infn.it/t0d1/lhcb/test/roberto/zz_zz.dst" ; export nodename="storm-fe-lhcb.cr.cnaf.infn.it"   ;;
    uk)    export SURL="srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/lhcb/DST/test/roberto/zz_zz.dst"; export nodename="srm-lhcb.gridpp.rl.ac.uk"  ;;
    es)    export SURL="srm://srmlhcb.pic.es/pnfs/pic.es/data/lhcb/CCRC08/DST/test/roberto/zz_zz.dst"; export nodename="srmlhcb.pic.es"  ;;
    de)    export SURL="srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/disk-only/dst/test/roberto/zz_zz.dst"; export nodename="gridka-dCache.fzk.de"   ;;
    fr)    export SURL="srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/data/CCRC08/DST/test/roberto/zz_zz.dst"; export nodename="ccsrm.in2p3.fr"   ;;
    nl)    export SURL="srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lhcb/DST/test/roberto/zz_zz.dst"; export nodename="srm.grid.sara.nl"  ;; esac


for i in $protocols; do 
    TURLO=`lcg-gt -t 300 -D srmv2 -T srmv2 $SURL $i`; 
    if [ "$?" == "0" ];then 
    num=`echo $TURLO |grep gsiftp | wc -l`
	if [ "$num" != "1" ]; then 
		export PROTOCOL=$i
		break
	fi
    fi
done;

echo "Protocol Found:$PROTOCOL" >>SAMOUT

echo "SURL:$SURL" >>SAMOUT
echo "NODENAME:$nodename" >> SAMOUT

if [ "x$PROTOCOL" == "x" ]; then 
    echo "none of the protocols in this list are suitable" >>SAMOUT
    echo "{dcap gsidcap file root rfio}" >> SAMOUT
    echo "SAMRESULT:40" >>SAMOUT
    exit $SAME_ERROR
fi

#now downloading root software....

wget http://grid-deployment.web.cern.ch/grid-deployment/eis/docs/SAM/root.tgz > /dev/null 2>&1

export LCG_GFAL_VO=lhcb
export LCG_CATALOG_TYPE=lfc

#now unpacking and setting the environment correctly

tar zxf root.tgz > /dev/null 2>&1
rm -rf  root.tgz > /dev/null 2>&1

export ROOTSYS=`pwd`/root
export LD_LIBRARY_PATH=$ROOTSYS/lib:`pwd`/lib:${LD_LIBRARY_PATH}
export PATH=$ROOTSYS/bin:`pwd`/bin:${PATH}
rm rdfile.C  > /dev/null 2>&1

#Now running lcg-gt to retrieve the tURL and handling a bit more

export TURL=`lcg-gt -t 300 -D srmv2 -T srmv2 $SURL $PROTOCOL | awk ' NR == 1' 2>>SAMOUT`

echo $TURL | grep "gsidcap:" > /dev/null 2>&1
if [ "$?" == "0" ];then 
  export TURL="dcap:$TURL"
fi

echo $TURL >> SAMOUT

if [ "x$TURL" == "x" ]; then
 echo "No turl available in 300 seconds: problem with lcg-gt" >>SAMOUT
  echo "A second trial with 60 seconds timeout gives now" >>SAMOUT
  lcg-gt -t 60 -D srmv2 -T srmv2 $SURL $PROTOCOL >> SAMOUT 2>&1
  echo "using the lcg_utils version" >> SAMOUT
  lcg-gt --version >>SAMOUT
   echo "SAMRESULT:50" >>SAMOUT
   exit $SAME_ERROR
fi 
 # writing the root macro for processing tURL

echo "void rdfile()" > rdfile.C
echo "{" >> rdfile.C
echo 'TFile* f = TFile::Open("'"$TURL"'");' >> rdfile.C
echo "f->ls();" >> rdfile.C
echo "exit(0);" >> rdfile.C
echo "}" >> rdfile.C
chmod a+x rdfile.C

 #running root against the tURL returned by SRM

root -b -q rdfile.C  >> SAMOUT 2>&1

if [ $? == "0" ];then 
  echo "File correctly open and read by ROOT" >> SAMOUT
  echo "SAMRESULT:10" >>SAMOUT  
  exit $SAME_OK
else
  echo "File access problems" >>SAMOUT
  echo "SAMRESULT:20" >>SAMOUT
  exit $SAME_INFO  
fi

