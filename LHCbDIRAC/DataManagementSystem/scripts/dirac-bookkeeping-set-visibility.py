#! /usr/bin/env python
"""
   Set the visibility flag to a dataset
"""

__RCSID__ = "$Id:  $"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript


if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()

  visibilityFlag = True
  Script.registerSwitch( '', 'Invisible', '   Set invisible rather than visible' )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Invisible':
      visibilityFlag = False

  bkQuery = dmScript.getBKQuery( visible = not visibilityFlag )
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()

  print "BQ query:", bkQuery
  lfns = bkQuery.getLFNs()
  if not lfns:
    print "No files found..."
  else:
    res = {'OK': True}
    if visibilityFlag:
      res = bk.setFilesVisible( lfns )
      msg = 'visible'
    else:
      res = bk.setFilesInvisible( lfns )
      msg = 'invisible'
    if not res['OK']:
      print "Error setting files %s" % msg
      DIRAC.exit( 1 )
    print "Successfully set %d files %s" % ( len( lfns ), msg )
