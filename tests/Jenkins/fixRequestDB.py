#!/usr/bin/env python

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
else:
  extension, system = result['Value']
  print InstallTools.addDatabaseOptionsToCS( gConfig, 'RequestManagement', db, overwrite = True ) 