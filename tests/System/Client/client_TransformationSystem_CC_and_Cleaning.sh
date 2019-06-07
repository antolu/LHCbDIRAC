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


transID=`cat TransformationID`
LFNlist=$(cat TransformationSystemTest/LFNlist.txt | awk -vORS=, '{print $1}')


echo ""
echo "Running Consistency Checks:"



"dirac-dms-check-directory-content.py"

"dirac-dms-check-directory-integrity.py"

"dirac-dms-check-bkk2fc.py"
dirac-dms-check-bkk2fc 
"dirac-dms-check-fc2bkk.py"

echo "dirac-dms-check-fc2se --Directory=$directory"
dirac-dms-check-fc2se --Directory=$directory

"dirac-dms-check-file-integrity.py"


# "dirac-dms-check-inputdata.py"

"dirac-production-verify-outputdata"
dirac-production-verify-outputdata $transID

echo ""
echo "Replicating Transformation Output:"





echo ""
echo "Deleting Transformation Output:"

"Running: dirac-production-remove-output $transID"



echo ""
echo "Cleanning Production ID: $transID"

"Running: dirac-transformation-clean $transID"

