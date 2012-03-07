#!/usr/bin/env python
########################################################################
# $HeadURL$
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

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN[,LFN2[,LFN3...]] [SE[,SE2]]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name or file containing LFNs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  seList = dmScript.getOption('SEs',[])

  args = Script.getPositionalArgs()
  if not seList:
    sites = dmScript.getOption( 'Sites', [] )
    from DIRAC import gConfig
    for site in sites:
      res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
      if not res['OK']:
        print 'Site %s not known' % site
        Script.showHelp()
      seList.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )
  if not seList:
    if not args:
      print "Give SE name as last argument or with --SE option"
      Script.showHelp()
      DIRAC.exit(0)
    seList = args.pop(-1).split(',')
  if type(seList) == type(''):
    seList = [seList]
  # This should be improved, with disk SEs first...
  seList.sort()

  lfns = dmScript.getOption('LFNs',[])
  lfns += args
  lfnList = []
  for lfn in lfns:
    try:
      f = open(lfn, 'r')
      lfnList += f.read().splitlines()
      f.close()
    except:
      lfnList.append(lfn)

  from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
  from DIRAC import gLogger
  if len(seList) > 1:
    gLogger.info( "Using the following list of SEs: %s" %str(seList))
  rm = ReplicaManager()
  gLogger.setLevel("FATAL")
  for se in seList:
    res = rm.getReplicaAccessUrl( lfnList, se )
    if res['OK']:
      printDMResult( res, empty="File not at SE")
      lfnList = res['Value']['Failed']
      if not lfnList: break

  if not res['OK']:
    gLogger.fatal( "Error getting accessURL for %s at %s" %(lfnList,str(seList)))
    printDMResult( res, empty="File not at SE", script="dirac-dms-lfn-accessURL")
  DIRAC.exit(0)
