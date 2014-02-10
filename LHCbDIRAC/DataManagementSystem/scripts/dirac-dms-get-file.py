#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-get-file
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve a single file or list of files from Grid storage to the current directory.
"""
__RCSID__ = "$Id$"
import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
from DIRAC.Core.Base import Script
import os

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerNamespaceSwitches( 'download to (default = %s)' % os.path.realpath( '.' ) )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )
  for lfn in Script.getPositionalArgs():
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  dirList = dmScript.getOption( 'Directory', [''] )
  from DIRAC import gLogger
  if len( dirList ) > 1:
    gLogger.always( "Not allowed to specify more than one destination directory" )
    DIRAC.exit( 2 )

  from DIRAC.DataManagementSystem.Client.DataManager                  import DataManager
  dm = DataManager()
  res = dm.getFile( lfnList, destinationDir = dirList[0] )
  DIRAC.exit( printDMResult( res,
                             empty = "No allowed replica found", script = "dirac-dms-get-file" ) )
