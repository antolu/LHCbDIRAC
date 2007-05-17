DIRAC_CVS_GET() {
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

  DIRAC_CVS_GET $1
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
