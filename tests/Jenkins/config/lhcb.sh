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


}

lhcbdirac_integration_scripts(){

  cd $WORKSPACE
  
  scripts=`ls LHCbDIRAC/*/scripts/dirac*.py`

  for script in $scripts
  do
    mv $script $(echo $script | sed 's/-/_/g' )
  done

  dirs=`ls LHCbDIRAC/*/scripts -d`
  for dir in $dirs
  do
    touch $dir/__init__.py
  done

}

#-------------------------------------------------------------------------------
#EOF