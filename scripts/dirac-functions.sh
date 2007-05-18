export DIRAC_CVS_TAG=HEAD
export DIRAC_CVS_LOGIN=0
export CVSROOT=':pserver:anonymous@isscvs.cern.ch:/local/reps/dirac'

TMP_DIR=/tmp/${USER}/$$
rm -rf ${TMP_DIR}
mkdir -p ${TMP_DIR}
cd $TMP_DIR

DIRAC_CVS_GET() {
  rm -rf $1
  if [ $DIRAC_CVS_LOGIN -eq 0 ] ; then
    cvs login && export DIRAC_CVS_LOGIN=1
  fi
  if cvs export -f -r ${DIRAC_CVS_TAG} $1 1>> DIRAC_CVS_GET.log 2>> DIRAC_CVS_GET.log; then
    echo " $1 retrieved from CVS"
  else
    echo " Failed to retrieved $1"
    exit -1
  fi
}

DIRAC_MAKE() {

  [ -d $1 ] || DIRAC_CVS_GET $1
  ( 
    cd $1
    if ./dirac-make 1>> dirac-make.log 2>> dirac-make.log ; then
      echo " $1 successfully made"
    else
      echo " Failed to run dirac-make for $1"
      exit -1
    fi
  )
}

DIRAC_INSTALL() {

  if [ -d $1 ] ; then
    DIRAC_MAKE $1
    return
  fi
  ( 
    cd $1
    if ./dirac-install 1>> dirac-install.log 2>> dirac-install.log ; then
      echo " $1 successfully installed"
    else
      echo " Failed to run dirac-install for $1"
      exit -1
    fi
  )
}


DIRAC_TAR() {

  tarfile=$1
  shift

  (
    cd DIRAC3
    echo 
    echo "Creating tar file $tarfile"
    echo " using: $*"
    if tar -czf $tarfile $* ; then
			echo
    else
      echo " Failed to create $tarfile"
      exit -1
    fi
  )
}