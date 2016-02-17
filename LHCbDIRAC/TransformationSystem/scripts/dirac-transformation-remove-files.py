#!/usr/bin/env python

"""
 Set files Removed in a transformation
"""

__RCSID__ = "$Id$"

def __getTransformations( args ):
  transList = []
  if not len( args ):
    print "Specify transformation number..."
    Script.showHelp()
  else:
    ids = args[0].split( "," )
    try:
      for transID in ids:
        r = transID.split( ':' )
        if len( r ) > 1:
          for i in range( int( r[0] ), int( r[1] ) + 1 ):
            transList.append( i )
        else:
          transList.append( int( r[0] ) )
    except Exception as e:
      gLogger.exception( "Invalid transformation", lException = e )
      transList = []
  return transList

if __name__ == "__main__":
  import os
  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.parseCommandLine( ignoreErrors = True )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  runInfo = True
  userGroup = None

  transList = __getTransformations( Script.getPositionalArgs() )
  if not transList:
    DIRAC.exit( 1 )

  requestedLFNs = dmScript.getOption( 'LFNs' )
  if not requestedLFNs:
    gLogger.always( 'No files to add' )
    DIRAC.exit( 1 )

  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  trClient = TransformationClient()
  rc = 0
  for transID in transList:
    res = trClient.setFileStatusForTransformation( transID, 'Removed', requestedLFNs, force = True )
    if res['OK']:
      gLogger.always( 'Successfully set %d files%s Removed in transformation %d' % ( len( res['Value'] ),
                                                                                     ( ' (out of %d)' % len( requestedLFNs ) ) if len( res['Value'] ) != len( requestedLFNs ) else '',
                                                                                     transID ) )
    else:
      gLogger.always( 'Failed to set %d files Removed in transformation %d' % ( len( requestedLFNs ), transID ), res['Message'] )
      rc = 2
  DIRAC.exit( rc )
