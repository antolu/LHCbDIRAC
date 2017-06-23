#!/usr/bin/env python
"""
  Returns the platform supported by the current WN
  If requested (--Full), it returns the list of LHCb configs compatible with that platform
  If a (list of) LHCb configs is given as positional argument, it checks if hte WN is compatible with them
"""

__RCSID__ = "$Id$"

if __name__ == "__main__" :
  from DIRAC.Core.Base.Script import Script
  Script.registerSwitch( '', 'Full', '   Print additional information on compatible LHCb platforms' )
  Script.parseCommandLine( ignoreErrors = True )

  full = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Full':
      full = True

  from DIRAC import exit as dExit
  from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getPlatform, getLHCbConfigsForPlatform, getPlatformFromLHCbConfig
  from DIRAC import gLogger
  try:
    # Get the platform name. If an error occurs, an exception is thrown
    platform = getPlatform()
    if not platform:
      gLogger.fatal( "There is no platform corresponding to this machine" )
      DIRAC.exit( 1 )
    if not full:
      print platform
      dExit( 0 )

  except Exception as e:
    msg = "Exception getting platform: " + repr( e )

    from DIRAC import gConfig
    from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

    mailAddress = Operations().getValue( 'EMail/JobFailures', 'Vladimir.Romanovskiy@cern.ch' )
    site = gConfig.getValue( 'LocalSite/Site' )
    ce = gConfig.getValue( 'LocalSite/GridCE' )
    queue = gConfig.getValue( 'LocalSite/CEQueue' )
    body = "*** THIS IS AN AUTOMATED MESSAGE ***" + '\n\n' + msg + '\n\n'
    body = body + "At site %s, CE = %s, queue = %s" % ( site, ce, queue ) + '\n\n'
    body = body + "Consider inserting it in the OSCompatibility section of the CS"

    for mA in mailAddress.replace( ' ', '' ).split( ',' ):
      NotificationClient().sendMail( mailAddress, "Problem with DIRAC architecture",
                                     body, 'federico.stagni@cern.ch', localAttempt = False )
    print msg
    dExit( 1 )

  # This is for printing additional information if required
  args = Script.getPositionalArgs()
  if args:
    configurations = set( config.strip() for arg in args for config in arg.split( ',' ) )
  else:
    configurations = None


  gLogger.notice( "This machine is platform", platform )
  compatibleConfigs = getLHCbConfigsForPlatform( platform ).get( 'Value' )
  if not compatibleConfigs:
    gLogger.notice( "No compatible configurations found" )
    DIRAC.exit( 0 )

  if configurations:
    # If a list was given, check which ones are compatible
    compatibleConfigs = set( compatibleConfigs )
    incompatible = configurations - compatibleConfigs
    compatibleConfigs &= configurations
    if incompatible:
      gLogger.notice( "Some configurations are not compatible with the current platform (%s):" % platform, ', '.join( sorted( incompatible ) ) )
      required = set( getPlatformFromLHCbConfig( config ) for config in incompatible ) - set( [None] )
      if required:
        gLogger.notice( "Would require platform", ', '.join( sorted( required ) ) )
      else:
        gLogger.notice( "No suitable platform found" )
  gLogger.notice( "All compatible configurations:", ', '.join( sorted( compatibleConfigs ) ) )
  DIRAC.exit( 0 )
