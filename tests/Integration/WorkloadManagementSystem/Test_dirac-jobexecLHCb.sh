#!/bin/sh
#-------------------------------------------------------------------------------

# Tests that require a $DIRACSCRIPTS pointing to DIRAC deployed scripts location,
# and a $DIRAC variable pointing to an installed DIRAC
# It also assumes that pilot.cfg contains all the necessary for running

echo -e "\n======> Test_dirac-jobexecLHCb.sh <======\n"

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
echo -e "\n==> jobDescriptionLHCb-OK.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-OK.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
ret_code=$?
if [ $ret_code -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!"
  echo -e "ret_code = $ret_code \n\n"
  more jobDescriptionLHCb-OK.xml
  exit $ret_code
fi

# OK2
echo -e "\n==> jobDescriptionLHCb-multiSteps-OK.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-multiSteps-OK.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
ret_code=$?
if [ $ret_code -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!"
  echo -e "ret_code = $ret_code \n\n"
  more jobDescriptionLHCb-multiSteps-OK.xml
  exit $ret_code
fi


# # FAIL
echo -e "\n==> jobDescriptionLHCb-FAIL.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-FAIL.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
ret_code=$?
# for lb-run specific errors (e.g. 111 like here) we reschedule even for user jobs (LHCbScript)
# (exit code 1502 becomes 222 --- 1502 & 255 (0xDE))
if [ $ret_code -eq 222 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!"
  echo -e "ret_code = $ret_code \n\n"
  more jobDescriptionLHCb-FAIL.xml
  exit $ret_code
fi

# # FAIL2
echo -e "\n==> jobDescriptionLHCb-multiSteps-FAIL.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-multiSteps-FAIL.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
ret_code=$?
# for lb-run specific errors (e.g. 111 like here) we reschedule even for user jobs (LHCbScript)
if [ $ret_code -eq 222 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  echo -e "ret_code = $ret_code \n\n"
  more jobDescriptionLHCb-multiSteps-FAIL.xml
  exit $ret_code
fi


# FAIL with exit code > 255
echo -e "\n==> jobDescriptionLHCb-FAIL1502.xml"
$DIRACSCRIPTS/dirac-jobexec jobDescriptionLHCb-FAIL1502.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
ret_code=$?
if [ $ret_code -eq 222 ] # This is 1502 & 255 (0xDE)
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!"
  echo -e "ret_code = $ret_code \n\n"
  more jobDescriptionLHCb-FAIL1502.xml
  exit $ret_code
fi

# # Removals
rm jobDescriptionLHCb-OK.xml
rm jobDescriptionLHCb-multiSteps-OK.xml
rm jobDescriptionLHCb-FAIL.xml
rm jobDescriptionLHCb-multiSteps-FAIL.xml
rm jobDescriptionLHCb-FAIL1502.xml
rm Script1_CodeOutput.log
