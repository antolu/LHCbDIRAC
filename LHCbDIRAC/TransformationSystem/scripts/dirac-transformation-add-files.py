#!/usr/bin/env python

"""
 Add files to a transformation
"""

__RCSID__ = "$Id$"

def __getTransformations( args ):
  if not len( args ):
    print "Specify transformation number..."
    Script.showHelp()
  else:
      ids = args[0].split( "," )
      transList = []
      for transID in ids:
          r = transID.split( ':' )
          if len( r ) > 1:
              for i in range( int( r[0] ), int( r[1] ) + 1 ):
                  transList.append( i )
          else:
              transList.append( int( r[0] ) )
  return transList

if __name__ == "__main__":

  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
  from LHCbDIRAC.TransformationSystem.Client.Utilities   import addFilesToTransformation
  import time

  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'NoRunInfo', '   Use if no run information is required' )

  Script.parseCommandLine( ignoreErrors = True )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )


  runInfo = True
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'NoRunInfo':
      runInfo = False

  args = Script.getPositionalArgs()

  requestedLFNs = dmScript.getOption( 'LFNs' )
  if not args:
    gLogger.always( 'No transformation specified' )
    Script.showHelp()
    DIRAC.exit( 1 )
  else:
    try:
      transID = int( args[0] )
    except:
      gLogger.always( 'Invalid transformation ID' )
      DIRAC.exit( 1 )
  if not requestedLFNs:
    gLogger.always( 'A list of files must be provided' )
    Script.showHelp()
    DIRAC.exit( 1 )

  res = addFilesToTransformation( transID, requestedLFNs, runInfo )
  if res['OK']:
    gLogger.always( 'Successfully added %d files to transformation %d' % ( len( res['Value'] ), transID ) )
    rc = 0
  else:
    gLogger.always( 'Failed to add %d files to transformation %d' % ( len( requestedLFNs ), transID ), res['Message'] )
    rc = 2
  DIRAC.exit( rc )
