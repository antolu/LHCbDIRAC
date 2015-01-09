#!/usr/bin/env python
########################################################################
# $HeadURL: $
# File :    dirac-bookkeeping-job-info
# Author :  Zoltan Mathe
########################################################################
"""
  It returns the job meta data for a given list of LFNs
"""
__RCSID__ = "$Id:  $"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
import DIRAC

if __name__ == "__main__":

  bkScript = DMScript()
  bkScript.registerFileSwitches()
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN|File]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs' ] ) )



  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  for lfn in args:
    bkScript.setLFNsFromFile( lfn )
  lfnList = bkScript.getOption( 'LFNs', [] )
  if not lfnList:
    Script.showHelp()
    DIRAC.exit( 0 )

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  retVal = BookkeepingClient().bulkJobInfo( lfnList )
  printDMResult( retVal, empty = "File does not exists in the Bookkeeping" )
