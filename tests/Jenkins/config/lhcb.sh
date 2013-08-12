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
    echo $script | cut -d '.' -f 1 >> $WORKSPACE/scripts_list.txt
  done

  dirs=`ls LHCbDIRAC/*/scripts -d`
  for dir in $dirs
  do
    touch $dir/__init__.py
  done 

}

lhcbdirac_branch_changelog(){

  tmpdir=`mktemp -d`
  cd $tmpdir
  svn co http://svn.cern.ch/guest/dirac/LHCbDIRAC/tags/LHCbDIRAC --depth=immediates -q .
  currentBranch=`echo $JOB_NAME | cut -d '-' -f 3`
  
  #previous=`ls -d $currentBranch* | grep -v pre`
  
  previous=`ls -d $currentBranch* | grep -v pre | sort -n -t p -k 2`
  prevTag=`echo $previous | rev | cut -d ' ' -f 1 | rev`
  #prevTag=`echo "$previous" | sort -n -t p -k 2 | rev | cut -d ' ' -f 1 | rev`  
  
  cd $WORKSPACE
  
#  echo $previous > $WORKSPACE/previousBranches.txt
  echo $prevTag  > $WORKSPACE/prevTag.txt
  
  prevRev=`svn log http://svn.cern.ch/guest/dirac/LHCbDIRAC/tags/LHCbDIRAC/$prevTag --stop-on-copy --xml --with-no-revprops | grep revision | head -n 1 | cut -d '"' -f 2`
  
  echo "Getting changelog between $prevRev and $SVN_REVISION"
  
  svn log http://svn.cern.ch/guest/dirac/LHCbDIRAC/branches/LHCbDIRAC_$currentBranch_branch -r $prevRev:$SVN_REVISION -v --xml > xmlFile.xml
  
  


}


#-------------------------------------------------------------------------------
#EOF