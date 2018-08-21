#!/bin/sh
#-------------------------------------------------------------------------------

# Tests that require a $DIRACSCRIPTS pointing to DIRAC deployed scripts location,
# and a $DIRAC variable pointing to an installed DIRAC
# It also assumes that pilot.cfg contains all the necessary for running

echo "\n======> Test_dirac-jobexecLHCb.sh <======\n"

if [ ! -z "$DEBUG" ]
then
  echo '==> Running in DEBUG mode'
  DEBUG='-ddd'
else
  echo '==> Running in non-DEBUG mode'
fi

# Creating the XML job description files
python $DIRAC/LHCbDIRAC/tests/Integration/WorkloadManagementSystem/createJobXMLDescriptionsLHCb.py $DEBUG

###############################################################################
# Running the real tests

# OK
echo "\n==> jobDescriptionLHCb-OK.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-OK.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [ $? -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

# OK2
echo "\n==> jobDescriptionLHCb-multiSteps-OK.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-multiSteps-OK.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [ $? -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi


# # FAIL
echo "\n==> jobDescriptionLHCb-FAIL.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-FAIL.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [ $? -eq 111 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

# # FAIL2
echo "\n==> jobDescriptionLHCb-multiSteps-FAIL.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-multiSteps-FAIL.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [ $? -eq 111 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi


# FAIL with exit code > 255
echo "\n==> jobDescriptionLHCb-FAIL1502.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-FAIL1502.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [ $? -eq 222 ] # This is 1502 & 255 (0xDE)
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

# # Removals
rm jobDescriptionLHCb-OK.xml
rm jobDescriptionLHCb-multiSteps-OK.xml
rm jobDescriptionLHCb-FAIL.xml
rm jobDescriptionLHCb-multiSteps-FAIL.xml
rm jobDescriptionLHCb-FAIL1502.xml
rm Script1_CodeOutput.log
