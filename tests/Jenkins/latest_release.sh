#!/bin/sh
#-------------------------------------------------------------------------------
# latest_release
#  
# : gets releases.cfg and writes the result into two files
# : dirac.version and lhcbdirac.version
#  
# ubeda@cern.ch  
# 10/VI/2013
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# Parse Input Parameters
#-------------------------------------------------------------------------------

# Execute getopt
ARGS=`getopt -o "p" -l "pre" -n "getopt.sh" -- "$@"`
 
#Bad arguments
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
 
# A little magic
eval set -- "$ARGS"

PRE='p[[:digit:]]*' 
 
# Now go through all the options
while true;
do
  case "$1" in
    -p|--pre) PRE='-pre'; shift 2 ;;
    --) shift      ; break   ;;
    *)  break ;;
  esac
done

#-------------------------------------------------------------------------------
# Get releases.cfg
#-------------------------------------------------------------------------------

tmp_dir=`mktemp -d -q`
  
cd $tmp_dir

wget http://svn.cern.ch/guest/dirac/LHCbDIRAC/trunk/LHCbDIRAC/releases.cfg --quiet

project=`cat releases.cfg | grep [^:]v[[:digit:]]r[[:digit:]]*$PRE | head -1 | sed 's/ //g'`

s=`cat releases.cfg | grep -n $project | cut -d ':' -f 1 | head -1`  
s=$(($s+2))
e=$(($s+3))
versions=`sed -n "$s,$e p" releases.cfg`

dirac=`echo $versions | tr ' ' '\n' | grep ^DIRAC:v*[^,] | sed 's/,//g' | cut -d ':' -f2`
lhcbdirac=`echo $versions | tr ' ' '\n' | grep ^LHCbDIRAC:v* | sed 's/,//g' | cut -d ':' -f2`
  
cd - >> /dev/null 
rm -r $tmp_dir

echo PROJECT:$project && echo $project > project.version
echo DIRAC:$dirac && echo $dirac > dirac.version
echo LHCbDIRAC:$lhcbdirac && echo $lhcbdirac > lhcbdirac.version

#-------------------------------------------------------------------------------
#EOF