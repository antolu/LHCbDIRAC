#!/bin/bash

echo "Submitting test production"
python dirac-test-production.py -ddd
if [ $? -ne 0 ]
then
   exit $?
fi
