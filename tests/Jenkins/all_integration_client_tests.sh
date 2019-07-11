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
# A convenient way to run all the LHCbDIRAC integration tests for client -> server interaction
#
# It supposes that LHCbDIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------


echo -e '****************************************'
echo -e '******' "LHCb client -> server tests" '******\n'


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** LHCb Bookkeeping TESTS ****\n"
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/BookkeepingSystem/Test_Bookkeeping.py >> testOutputs.txt 2>&1
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/BookkeepingSystem/Test_BookkeepingGUImethods.py >> testOutputs.txt 2>&1

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** LHCb PMS TESTS ****\n"
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/ProductionManagementSystem/Test_ProductionRequest.py >> testOutputs.txt 2>&1
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/ProductionManagementSystem/Test_Client_MCStatsElasticDB.py >> testOutputs.txt 2>&1

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** LHCb RSS TESTS ****\n"
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/ResourceStatusSystem/Test_ResourceManagement.py >> testOutputs.txt 2>&1

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** LHCb TS TESTS ****\n"
python $SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/TransformationSystem/Test_ClientTransformation.py >> testOutputs.txt 2>&1

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** LHCb WMS TESTS ****\n"
$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/WorkloadManagementSystem/Test_dirac-jobexecLHCb.sh >> testOutputs.txt 2>&1
