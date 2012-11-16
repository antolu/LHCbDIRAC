#! /usr/bin/env python
'''
   Set the replica flag for output files of a transformation that are in the LFC and not in the BK
   <transList> is a comma-separated list of transformation ID or ranges (<t1>:<t2>)

   It also does the opposite, when a file has ReplicaFlag=Yes when it shouldn't.
'''

#Script initialization
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] <transList>' % Script.scriptName, ] ) )
Script.parseCommandLine( ignoreErrors = False )

#imports
import sys, os
import DIRAC
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

#Code
if __name__ == "__main__":


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
    cc = ConsistencyChecks( transformationID, bkClient = bkClient )
    gLogger.always( "Processing %s production %d" % ( cc.transType, cc.prod ) )
    cc.checkBKK2FC()
    if cc.existingLFNsWithBKKReplicaNO:
      gLogger.always( "Setting the replica flag to %d files: %s" % ( len( cc.existingLFNsWithBKKReplicaNO ),
                                                                     str( cc.existingLFNsWithBKKReplicaNO ) ) )
      res = bkClient.addFiles( cc.existingLFNsWithBKKReplicaNO )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
    if cc.nonExistingLFNsWithBKKReplicaYES:
      gLogger.always( "Un-Setting the replica flag to %d files: %s" % ( len( cc.nonExistingLFNsWithBKKReplicaYES ),
                                                                        str( cc.nonExistingLFNsWithBKKReplicaYES ) ) )
      res = bkClient.removeFiles( cc.nonExistingLFNsWithBKKReplicaYES )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
    gLogger.always( "Processed production %d" % cc.prod )
