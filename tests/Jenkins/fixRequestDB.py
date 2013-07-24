#!/usr/bin/env python

# OBSOLETE !

import sys

from DIRAC.Core.Utilities import InstallTools
from DIRAC import gConfig
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.parseCommandLine()

InstallTools.getMySQLPasswords()

db = 'RequestDB'

result = InstallTools.installDatabase( db )
if not result['OK']:
  print "ERROR: failed to correctly install %s" % db, result['Message']
  sys.exit( result[ 'Message' ] )
else:
  extension, system = result['Value']
  print InstallTools.addDatabaseOptionsToCS( gConfig, 'RequestManagement', db, overwrite = True )
  
sys.exit( 0 )   