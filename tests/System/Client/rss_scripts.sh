#!/bin/bash
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

'
This is a script that tests:
dirac-rss-query-db,
dirac-rss-list-status,,
dirac-rss-set-token,
dirac-rss-query-dtcache

It is based on script outputs and exit codes
'

echo -e "\n\n TESTING: dirac-rss-query-db --name=test123 --status=Banned --statusType=ReadAccess --elementType=StorageElement --reason=test add resource status"
dirac-rss-query-db --name=test123 --status=Banned --statusType=ReadAccess --elementType=StorageElement --reason=test add resource status
if [ $? -ne 0 ]
then
  echo -e "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-list-status --name=test123 --element=Resource"
dirac-rss-list-status --name=test123 --element=Resource -dd
if [ $? -ne 0 ]
then
  echo -e "Script dirac-rss-list-status did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-set-token --name=test123 --element=Resource --reason=RSStest --releaseToken"
TEST_OUT=$( dirac-rss-set-token --name=test123 --element=Resource --reason=RSStest --releaseToken -dd )
if [ $? -ne 0 ]
then
  echo -e "Script dirac-rss-set-token did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-list-status --name=test123 --element=Resource -dd"
TEST_OUT=$( dirac-rss-list-status --name=test123 --element=Resource -dd )
if [[ $TEST_OUT != *"rs_svc"* ]]
then
  echo -e "Script dirac-rss-set-token did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo -e "\n\n TESTING: dirac-rss-query-db --name=test123 delete resource status -dd"
TEST_OUT=$( dirac-rss-query-db --name=test123 delete resource status -dd )
if [[ $TEST_OUT != *"successfully executed"* ]]
then
  echo -e "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo -e "\n\n TESTING: dirac-rss-list-status --name=test123 --element=Resource -dd"
TEST_OUT=$( dirac-rss-list-status --name=test123 --element=Resource -dd )
if [[ $TEST_OUT != *"No output"* ]]
then
  echo -e "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 add"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 add -dd )

if [[ $TEST_OUT != *"successfully executed"* ]]
then
  echo -e "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 select"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 select )
if [[ $TEST_OUT != *"4354354789"* ]]
then
  echo -e "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 delete -dd"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 delete -dd )
if [[ $TEST_OUT != *"successfully executed"* ]]
then
  echo -e "\n\nScript dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 select"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 select )
if [[ $TEST_OUT != *"number: 0"* ]]
then
  echo -e "\n\nScript dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi
