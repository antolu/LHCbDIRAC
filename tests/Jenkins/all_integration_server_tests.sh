#!/bin/sh
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################

#-------------------------------------------------------------------------------
# A convenient way to run all the LHCbDIRAC integration tests for servers
#
# It supposes that LHCbDIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------


echo -e '****************************************'
echo -e '********' "LHCb server tests" '*********\n'


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** LHCb Accounting TESTS ****\n"
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/AccountingSystem/Test_Plotter.py >> testOutputs.txt 2>&1

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** LHCb DMS TESTS ****\n"
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/DataManagementSystem/Test_RAWIntegrity.py >> testOutputs.txt 2>&1

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** LHCb PMS TESTS ****\n"
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/ProductionManagementSystem/Test_MCStatsElasticDB.py >> testOutputs.txt 2>&1
TESTCODE=$TESTCODE python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/ProductionXMLLogAnalysis/Test_XMLSummaryAnalysis.py >> testOutputs.txt 2>&1
