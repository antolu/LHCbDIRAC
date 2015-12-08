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
        for i in xrange( int( r[0] ), int( r[1] ) + 1 ):
          transList.append( i )
      else:
        transList.append( int( r[0] ) )
  return transList

if __name__ == "__main__":
  import os
  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'NoRunInfo', '   Use if no run information is required' )
  Script.registerSwitch( "", "Chown=", "   Give user/group for chown of the directories of files in the FC" )

  Script.parseCommandLine( ignoreErrors = True )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  runInfo = True
  userGroup = None

  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'NoRunInfo':
      runInfo = False
    elif opt == 'Chown':
      userGroup = val.split( '/' )
      if len( userGroup ) != 2 or not userGroup[1].startswith( 'lhcb_' ):
        gLogger.fatal( "Wrong user/group" )
        DIRAC.exit( 2 )

  if userGroup:
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    res = getProxyInfo()
    if not res['OK']:
      gLogger.fatal( "Can't get proxy info", res['Message'] )
      exit( 1 )
    properties = res['Value'].get( 'groupProperties', [] )
    if not 'FileCatalogManagement' in properties:
      gLogger.error( "You need to use a proxy from a group with FileCatalogManagement" )
      exit( 5 )

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
    gLogger.always( 'No files to add' )
    DIRAC.exit( 1 )

  from LHCbDIRAC.DataManagementSystem.Utilities.FCUtilities import chown
  from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities   import addFilesToTransformation
  if userGroup:
    directories = set( [os.path.dirname( lfn ) for lfn in requestedLFNs] )
    res = chown( directories, user = userGroup[0], group = userGroup[1] )
    if not res['OK']:
      gLogger.fatal( "Error changing ownership", res['Message'] )
      DIRAC.exit( 3 )
    gLogger.notice( "Successfully changed owner/group for %d directories" % res['Value'] )
  res = addFilesToTransformation( transID, requestedLFNs, runInfo )
  if res['OK']:
    gLogger.always( 'Successfully added %d files to transformation %d' % ( len( res['Value'] ), transID ) )
    rc = 0
  else:
    gLogger.always( 'Failed to add %d files to transformation %d' % ( len( requestedLFNs ), transID ), res['Message'] )
    rc = 2
  DIRAC.exit( rc )
