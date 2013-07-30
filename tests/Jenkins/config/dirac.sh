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
  else
    echo 'Fetching latest DIRAC'
    cd DIRAC
    git fetch origin
  fi
 
}


dirac_new_tag(){

  cd $WORKSPACE/DIRAC

  DIRACVERSION=`echo $JOB_NAME | cut -d '_' -f 2`
  
  currentBranch=`git branch | grep '*' | cut -d ' ' -f 2`
  echo currentBranch $currentBranch
  
  tags="`git tag | grep $DIRACVERSION | sort -n -t p -k 2`"

  nonPreRelease=`echo "$tags" | grep -v pre`
  if [ ! $"nonPreRelease" ]
  then
    echo "We are on PRE-Release"
    #tags=$'integration\n'$tags
  else
    echo "We are on PRODUCTION"
    wereOnPreRelease=`echo $currentBranch|grep pre`
    [ $wereOnPreRelease ] && currentBranch=integration && echo "Moved from PRE-Release to PRODUCTION"
    #tags=$'integration\n'"$nonPreRelease"
    tags=$nonPreRelease
  fi

  #newTag=`echo $tags | awk -F "$currentBranch " '{ print $2 }' | cut -d ' ' -f 1`
  latestTag=`echo $tags | rev | cut -d ' ' -f 1 | rev`
  
  echo $currentBranch > ../current.txt
  echo $latestTag > ../new_tag.txt

  cd $WORKSPACE

}


dirac_branch_script_trigger(){

  dirac_branch
  dirac_new_tag
  new_tag=`cat $WORKSPACE/new_tag.txt`
  cur_tag=`cat $WORKSPACE/current.txt`
  [ $new_tag != $cur_tag ] && exit 0
  exit 1

}


dirac_branch_update_workspace(){

  dirac_branch
  dirac_new_tag
  
  new_tag=`cat $WORKSPACE/new_tag.txt`
  cur_tag=`cat $WORKSPACE/current.txt`
  
  cd $WORKSPACE/DIRAC
  ( [ $new_tag != $cur_tag ] && git checkout tags/$new_tag -b $new_tag ) || git checkout $cur_tag

  cd ..
  [ ! -e scripts ] && dirac_scripts
  [ ! -e Linux_x86_64_glibc-2.5 ] && dirac_externals
  

}


dirac_externals(){

  echo "Getting dirac externals"

  wget --no-check-certificate -O dirac-install 'https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py' --quiet
  python dirac-install -X -l DIRAC -r `cat $WORKSPACE/new_tag.txt`
  ( 
    . bashrc
    python `which easy_install` nose
    python `which easy_install` pylint  
  )
}


dirac_scripts(){

  echo "Getting dirac scripts hacked"

  cd $WORKSPACE
  mkdir scripts
  cp DIRAC/Core/scripts/dirac-platform.py scripts/dirac-platform

}

#-------------------------------------------------------------------------------
#EOF