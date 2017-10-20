#! /usr/bin/env python
"""
   Set the visibility flag to a dataset
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript


if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors=False )

  bkQuery = dmScript.getBKQuery()
  lfns = dmScript.getOption( 'LFNs', [] )
  if not bkQuery and not lfns:
    print "No BKQuery and no files given..."
    DIRAC.exit( 1 )
  # Invert the visibility flag as want to set Invisible those that are visible and vice-versa
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()

  visibilityFlag = dmScript.getOption( 'Visibility', None )
  if visibilityFlag is None:
    print 'Visibility option should be given'
    DIRAC.exit( 2 )
  visibilityFlag = bool( str( visibilityFlag ).lower() == 'yes' )
  if bkQuery:
    # Query with visibility opposite to what is requested to be set ;-)
    bkQuery.setOption( 'Visible', 'No' if visibilityFlag else 'Yes' )
    print "BQ query:", bkQuery
    lfns += bkQuery.getLFNs()
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
