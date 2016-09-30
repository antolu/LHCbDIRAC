#!/bin/bash

: '
This is a script that tests:
dirac-rss-query-db,
dirac-rss-list-status,,
dirac-rss-set-token,
dirac-rss-query-dtcache

It is based on script outputs and exit codes
'

echo "TESTING: dirac-rss-query-db --name=test123 --status=Banned --statusType=ReadAccess --reason=test add resource status"
TEST_OUT=$( dirac-rss-query-db --name=test123 --status=Banned --statusType=ReadAccess --elementType=StorageElement --reason=test add resource status )

if [[ $TEST_OUT != *"successfully executed"* ]]
then
  printf "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi


echo "TESTING: dirac-rss-list-status --name=test123 --element=Resource"
TEST_OUT=$( dirac-rss-list-status --name=test123 --element=Resource )

if [[ $TEST_OUT != *"Selection parameters"* ]]
then
  printf "Script dirac-rss-list-status did not get executed successfully \n"
  exit 1
fi

if [[ $TEST_OUT != *"ReadAccess | Banned | StorageElement"* ]]
then
  printf "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo "TESTING: dirac-rss-set-token --name=test123 --element=Resource --reason=RSStest --releaseToken"
TEST_OUT=$( dirac-rss-set-token --name=test123 --element=Resource --reason=RSStest --releaseToken )

if [[ -n $TEST_OUT ]]
then
  printf "Script dirac-rss-set-token did not get executed successfully \n"
  exit 1
fi

TEST_OUT=$( dirac-rss-list-status --name=test123 --element=Resource )
if [[ $TEST_OUT != *"rs_svc"* ]]
then
  printf "Script dirac-rss-set-token did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

TEST_OUT=$( dirac-rss-query-db --name=test123 delete resource status )
if [[ $TEST_OUT != *"successfully executed"* ]]
then
  printf "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

TEST_OUT=$( dirac-rss-list-status --name=test123 --element=Resource )
if [[ $TEST_OUT != *"No output"* ]]
then
  printf "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo "TESTING: dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 add"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 add )

if [[ $TEST_OUT != *"successfully executed"* ]]
then
  printf "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 select )
if [[ $TEST_OUT != *"4354354789"* ]]
then
  printf "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 delete )
if [[ $TEST_OUT != *"successfully executed"* ]]
then
  printf "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 select )
if [[ $TEST_OUT != *"number: 0"* ]]
then
  printf "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi
