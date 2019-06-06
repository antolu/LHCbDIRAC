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
echo "\n\n \t\t\t ===> Now some simple scripts  "
echo " "
echo "======  dirac-bookkeeping-run-files 81789"
dirac-bookkeeping-run-files 81789
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-bookkeeping-gui"
dirac-bookkeeping-gui
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
