#!/usr/bin/env python
########################################################################
# File :    dirac-dms-lfn-accessURL
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve an access URL for an LFN replica given a valid DIRAC SE.
"""
__RCSID__ = "$Id$"
import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
from DIRAC.Core.Base import Script

def __checkSEs( args ):
  seList = []
  res = gConfig.getSections( '/Resources/StorageElements' )
  if res['OK']:
    for ses in list( args ):
      sel = [se for se in ses.split( ',' ) if se in res['Value']]
      if sel :
        seList.append( ','.join( sel ) )
        args.remove( ses )
  return seList, args

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]] SE[,SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name or file containing LFNs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  seList = dmScript.getOption( 'SEs', [] )

  if not seList:
    sites = dmScript.getOption( 'Sites', [] )
    from DIRAC import gConfig
    for site in sites:
      res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
      if not res['OK']:
        print 'Site %s not known' % site
        Script.showHelp()
      seList.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )
  args = Script.getPositionalArgs()
  if not seList:
    seList, args = __checkSEs( args )
  # This should be improved, with disk SEs first...
  if not seList:
    print "Give SE name as last argument or with --SE option"
    Script.showHelp()
  seList.sort()

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
  from DIRAC import gLogger
  if len( seList ) > 1:
    gLogger.info( "Using the following list of SEs: %s" % str( seList ) )
  rm = ReplicaManager()
  gLogger.setLevel( "FATAL" )
  for se in seList:
    res = rm.getReplicaAccessUrl( lfnList, se )
    if res['OK']:
      printDMResult( res, empty = "File not at SE" )
      lfnList = res['Value']['Failed']
      if not lfnList: break

  if not res['OK']:
    gLogger.fatal( "Error getting accessURL for %s at %s" % ( lfnList, str( seList ) ) )
    printDMResult( res, empty = "File not at SE", script = "dirac-dms-lfn-accessURL" )
  DIRAC.exit( 0 )
