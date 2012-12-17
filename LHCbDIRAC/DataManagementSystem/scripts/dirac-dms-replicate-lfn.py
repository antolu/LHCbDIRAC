#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-replicate-lfn
# Author  : Stuart Paterson
########################################################################
"""
  Replicate an existing LFN to another Storage Element
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
from DIRAC import DIRAC, gConfig, gLogger
import os

def __checkSEs( args ):
  seList = []
  res = gConfig.getSections( '/Resources/StorageElements' )
  if res['OK']:
    for ses in list( args ):
      if [se for se in ses.split( ',' ) if se in res['Value']]:
        seList.append( ses )
        args.remove( ses )
  return seList, args

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...  [LFN1[,LFN2,[...]]] Dest [Source [Cache]]' % Script.scriptName,
                                       'Arguments:',
                                       '  Dest:     Valid DIRAC SE',
                                       '  Source:   Valid DIRAC SE',
                                       '  Cache:    Local directory to be used as cache' ] ) )
  Script.parseCommandLine( ignoreErrors=True )
  args = Script.getPositionalArgs()

  seList, args = __checkSEs( args )
  destList = []
  sourceSE = ''
  localCache = ''
  try:
    destList, xx = __checkSEs( seList[0].split( ',' ) )
    sourceSE = seList[1]
  except:
    pass
  #print seList, destList, sourceSE
  if not destList or len( sourceSE.split( ',' ) ) > 1:
    Script.showHelp()

  from DIRAC.Interfaces.API.Dirac                         import Dirac
  dirac = Dirac()

  if args:
    if os.path.exists( args[-1] ):
      localCache = args[-1]
      args.pop()

  lfns = dmScript.getOption( 'LFNs', [] )
  lfns += args
  #print lfns
  lfnList = []
  for lfn in lfns:
    try:
      f = open( lfn, 'r' )
      lfnList += [l.split( 'LFN:' )[-1].strip().split()[0].replace( '"', '' ).replace( ',', '' ) for l in f.read().splitlines()]
      f.close()
    except:
      lfnList.append( lfn )

  #print lfnList, destList, sourceSE, localCache
  finalResult = {'OK':True, 'Value':{"Failed":{}, "Successful":{}}}
  for lfn in lfnList:
    for seName in destList:
      result = dirac.replicateFile( lfn, seName, sourceSE, localCache, printOutput=False )
      if not result['OK']:
        finalResult['Value']["Failed"].setdefault( seName, {} ).update( {lfn:result['Message']} )
      else:
        if result['Value']['Failed']:
          finalResult['Value']['Failed'].setdefault( seName, {} ).update( result['Value']['Failed'] )
        if result['Value']['Successful']:
          finalResult['Value']['Successful'].setdefault( seName, {} ).update( result['Value']['Successful'] )

  DIRAC.exit( printDMResult( finalResult ) )
