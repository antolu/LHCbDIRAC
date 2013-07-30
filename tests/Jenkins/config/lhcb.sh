#!/bin/sh 
#-------------------------------------------------------------------------------
# dirac
#  
# : Script that contains the logic to run the LHCbDIRAC Jenkins tests.
#
#  
# ubeda@cern.ch  
# 30/VII/2013
#-------------------------------------------------------------------------------

lhcbdirac_integration_update_workspace(){

  cd $WORKSPACE

  rm -rf $WORKSPACE/LHCbDIRAC
  svn co http://svn.cern.ch/guest/dirac/LHCbDIRAC/trunk/LHCbDIRAC > /dev/null
  
  # from dirac.sh
  dirac_get DIRAC
  cd $WORKSPACE/DIRAC
  
  branch=rel-$1
  git checkout $branch
  git merge origin/$branch  
  

}

#-------------------------------------------------------------------------------
#EOF