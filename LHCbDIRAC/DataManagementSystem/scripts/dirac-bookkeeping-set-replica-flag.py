#! /usr/bin/env python
"""
   Set the replica flag for output files of a transformation that are in the LFC and not in the BK
   <transList> is a comma-separated list of transformation ID or ranges (<t1>:<t2>)

   It also does the opposite, when a file has ReplicaFlag=Yes when it shouldn't.
"""
import sys, os
from DIRAC import gLogger
from DIRAC.Core.Base import Script

from LHCbDIRAC.DataManagementSystem.Utilities.BKAndCatalogs import ConsistencyChecks
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

if __name__ == "__main__":

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] <transList>' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  args = Script.getPositionalArgs()

  if not len( args ):
    gLogger.warn( "Specify transformation number..." )
    DIRAC.exit( 0 )
  else:
    try:
      ids = args[0].split( "," )
      idList = []
      for id in ids:
        r = id.split( ':' )
        if len( r ) > 1:
          for i in range( int( r[0] ), int( r[1] ) + 1 ):
            idList.append( i )
        else:
          idList.append( int( r[0] ) )
    except:
      gLogger.error( "Invalid list of transformation (ranges): ", args[0] )
      Script.showHelp()
      DIRAC.exit( 1 )

  bkClient = BookkeepingClient()

  for transformationID in idList:
    cc = ConsistencyChecks( transformationID )
    cc.replicaConsistencyCheck()
    if cc.existingLFNsThatAreNotInBKK:
      gLogger.info( "Setting the replica flag to %d files" % len( cc.existingLFNsThatAreNotInBKK ) )
      res = bkClient.addFiles( cc.existingLFNsThatAreNotInBKK )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
    if cc.nonExistingLFNsThatAreInBKK:
      gLogger.info( "Un-Setting the replica flag to %d files" % len( cc.nonExistingLFNsThatAreInBKK ) )
      res = bkClient.removeFiles( cc.nonExistingLFNsThatAreInBKK )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
