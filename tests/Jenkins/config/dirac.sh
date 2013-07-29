#!/bin/sh 
#-------------------------------------------------------------------------------
# dirac
#  
# : Script that contains the logic to run the DIRAC Jenkins tests.
#
#  
# ubeda@cern.ch  
# 29/VII/2013
#-------------------------------------------------------------------------------

dirac_branch(){

  if [ ! -e DIRAC ]
  then
    echo 'Getting new DIRAC'
    git clone git://github.com/DIRACGrid/DIRAC.git
#    cd DIRAC
  else
    echo 'Fetching latest DIRAC'
    cd DIRAC
    git fetch origin
  fi
 
}

dirac_tags(){

  cd $WORKSPACE/DIRAC

  DIRACVERSION=`echo $JOB_NAME | cut -d '_' -f 2`
  
  currentBranch=`git branch | grep '*' | cut -d ' ' -f 2`
  echo currentBranch $currentBranch
  
  tags="`git tag | grep $DIRACVERSION | sort -n -t p -k 2`"

  nonPreRelease=`echo "$tags" | grep -v pre`
  if [ ! $nonPreRelease ]
  then
    echo "We are on PRE-Release"
    tags=$'integration\n'$tags

  else
    echo "We are on PRODUCTION"
    wereOnPreRelease=`echo $currentBranch|grep pre`
    [ $wereOnPreRelease ] && currentBranch=integration && echo "Moved from PRE-Release to PRODUCTION"
    tags=$'integration\n'"$nonPreRelease"
  fi

  echo $tags > dirac_tags

}

#-------------------------------------------------------------------------------
#EOF