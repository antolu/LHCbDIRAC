#!/bin/sh
#-------------------------------------------------------------------------------
# find_databases
#  
# : gets from DIRAC and any of the installed Extensions the .sql files which
# : correspond to a DB to be installed.
#  
# ubeda@cern.ch  
# 26/VI/2013
#-------------------------------------------------------------------------------

set -o errexit

#-------------------------------------------------------------------------------

find *DIRAC -name *DB.sql | uniq | awk -F "/" '{print $2,$4}' > databases

echo found `wc -l databases`

#-------------------------------------------------------------------------------

set +o errexit

#EOF