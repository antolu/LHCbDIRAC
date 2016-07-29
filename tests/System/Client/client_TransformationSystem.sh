#!/bin/bash

echo "Submitting test production"
python dirac-test-production.py -ddd
if [ $? -ne 0 ]
then
   exit $?
fi

transID=`cat TransformationID`

echo "Creating test files"
#FIXME: create unique files
date
#add them
dirac-dms-add-file

echo "Adding the files to the test production"
dirac-transformation-add-files $transID \
--LFNs=
if [ $? -ne 0 ]
then
   exit $?
fi
