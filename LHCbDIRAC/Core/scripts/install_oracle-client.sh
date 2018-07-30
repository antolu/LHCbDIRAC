#!/bin/bash
# Install Oracle client module on a CERN vobox machine
#
# 1) download oracle client tarbal
#curl http://prdownloads.sourceforge.net/cx-oracle/cx_Oracle-5.1.tar.gz -o cx_Oracle-5.1.tar.gz
curl http://lhcb-portal-dirac.cern.ch/defaults/cx_Oracle-5.1.tar.gz -o cx_Oracle-5.1.tar.gz
#
# 2) get CERN oracle client environment
source /afs/cern.ch/project/oracle/script/setoraenv.sh -s 11203
#
# 3) Install client module
python `which easy_install` cx_Oracle-5.1.tar.gz
#
exit 0
