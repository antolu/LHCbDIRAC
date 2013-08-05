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

dirac_get(){


  [ $1 ] && project=$1 || project=`echo $JOB_NAME | cut -d '-' -f 2`

  if [ ! -e $project ]
  then
    echo Getting new $project
    git clone git://github.com/DIRACGrid/$project.git
  else
    echo Fetching latest $project
    cd $project
    git fetch origin
  fi
 
}

#...............................................................................
#
# dirac_new_tag
#
# : writes new_tag.txt with the latest tag for the given $JOB_NAME
#...............................................................................

dirac_new_tag(){

  cd $WORKSPACE/DIRAC

  DIRACVERSION=`echo $JOB_NAME | cut -d '-' -f 3`
  
  currentBranch=`git branch | grep '*' | cut -d ' ' -f 2`
  echo currentBranch $currentBranch
  
  tags="`git tag | grep $DIRACVERSION`"

  nonPreRelease=`echo "$tags" | grep -v pre`
  if [ ! "$nonPreRelease" ]
  then
    echo "We are on PRE-Release"
    tags=`echo "$tags" | sort -n -t e -k 2`
  else
    echo "We are on PRODUCTION"
    wereOnPreRelease=`echo $currentBranch|grep pre`
    [ $wereOnPreRelease ] && currentBranch=integration && echo "Moved from PRE-Release to PRODUCTION"
    tags=`echo "$nonPreRelease" | sort -n -t p -k 2`
  fi

  latestTag=`echo $tags | rev | cut -d ' ' -f 1 | rev`
  
  echo $currentBranch > ../current.txt
  echo $latestTag > ../new_tag.txt

  cd $WORKSPACE

}


#...............................................................................
#
# dirac_branch_script_trigger
#
# : gets DIRAC and decides which is next tag to be taken into account. If there
# : is a new tag, it exits 0.
#...............................................................................

dirac_branch_script_trigger(){

  dirac_get DIRAC
  dirac_new_tag
  #new_tag=`cat $WORKSPACE/new_tag.txt`
  #cur_tag=`cat $WORKSPACE/current.txt`
  #[ $new_tag != $cur_tag ] && exit 0
  cd DIRAC
  new=`git log --since="1 hour ago"`
  
  [ "$new" ] && exit 0 || exit 1 

}


#...............................................................................
#
# dirac_branch_update_workspace
#
# : gets DIRAC and decides which is next tag to be taken into account. If needed
# : checksout a new version of the tag and creates Externals and Scripts ( if needed ).
#...............................................................................

dirac_branch_update_workspace(){

  dirac_get DIRAC
  dirac_new_tag
  
  new_tag=`cat $WORKSPACE/new_tag.txt`
  cur_tag=`cat $WORKSPACE/current.txt`
  
  cd $WORKSPACE/DIRAC
  ( [ $new_tag != $cur_tag ] && git checkout tags/$new_tag -b $new_tag ) || git checkout $cur_tag

  cd ..
  [ ! -e scripts ] && dirac_scripts
  [ ! -e Linux_x86_64_glibc-2.5 ] && dirac_externals
  

}

dirac_integration_update_workspace(){

  for project in $@
  do
    echo $project
    cd $WORKSPACE
    dirac_get $project 
    cd $WORKSPACE/$project
    git merge origin/integration
    cd $WORKSPACE
  done

  cd $WORKSPACE

  dirac_scripts  
  [ ! -e Linux_x86_64_glibc-2.5 ] && dirac_externals


}


dirac_integration_scripts(){

  cd $WORKSPACE
  
  scripts=`ls DIRAC/*/scripts/dirac*.py`

  for script in $scripts
  do
    mv $script $(echo $script | sed 's/-/_/g' )
  done

  dirs=`ls DIRAC/*/scripts -d`
  for dir in $dirs
  do
    touch $dir/__init__.py
  done

}

#...............................................................................
#
# dirac_externals
#
# : gets dirac-install from the repository and installs externals for the given
# : tag ( new_tag.txt )
#...............................................................................

dirac_externals(){

  echo "Getting dirac externals"
  
  wget --no-check-certificate -O dirac-install 'https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py' --quiet
  python dirac-install -X -l DIRAC -r `cat $WORKSPACE/new_tag.txt` -t server -p 'Linux_x86_64_glibc-2.5'
  ( 
    cd $WORKSPACE
    sed -i 's/`$DIRACSCRIPTS\/dirac-platform`/Linux_x86_64_glibc-2.5/g' bashrc
    . bashrc
    python `which easy_install` nose
    python `which easy_install` pylint
    python `which easy_install` mock
    python `which easy_install` PIL
    python `which easy_install` pyqt
  )
}


#...............................................................................
#
# dirac_scripts
#
# : mimics the scripts directory, putting on place dirac-platform needed by
# : bashrc
#...............................................................................

dirac_scripts(){

  echo "Getting dirac scripts hacked"

  cd $WORKSPACE
  [ ! -e scripts ] && mkdir scripts
  cp DIRAC/Core/scripts/dirac-platform.py scripts/dirac-platform

}


#-------------------------------------------------------------------------------
#EOF