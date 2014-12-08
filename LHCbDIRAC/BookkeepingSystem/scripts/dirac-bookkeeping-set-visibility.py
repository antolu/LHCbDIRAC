#! /usr/bin/env python
"""
   Set the visibility flag to a dataset
"""

__RCSID__ = "$Id: dirac-bookkeeping-set-visibility.py 69359 2013-08-08 13:57:13Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript


if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  bkQuery = dmScript.getBKQuery()
  if not bkQuery:
    print "No BKQuery given..."
    DIRAC.exit( 1 )
  # Invert the visibility flag as want to set Invisible those that are visible and vice-versa
  visibilityFlag = bkQuery.isVisible()
  bkQuery.setOption( 'Visible', 'No' if visibilityFlag else 'Yes' )
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
