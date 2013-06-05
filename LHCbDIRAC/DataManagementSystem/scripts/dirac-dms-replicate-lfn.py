#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-replicate-lfn
# Author  : Stuart Paterson
########################################################################
"""
  Replicate a (list of) existing LFN(s) to (set of) Storage Element(s)
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
      sel = [se for se in ses.split( ',' ) if se in res['Value']]
      if sel :
        seList.append( ','.join( sel ) )
        args.remove( ses )
  return seList, args

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ...  [LFN1[,LFN2,[...]]] Dest[,Dest2[,...]] [Source [Cache]]' % Script.scriptName,
                                       'Arguments:',
                                       '  Dest:     Valid DIRAC SE(s)',
                                       '  Source:   Valid DIRAC SE',
                                       '  Cache:    Local directory to be used as cache' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  seList, args = __checkSEs( Script.getPositionalArgs() )
  destList = []
  sourceSE = []
  localCache = ''
  try:
    destList = __checkSEs( seList[0].split( ',' ) )[0]
    sourceSE = seList[1].split( ',' )
  except:
    pass
  #print seList, destList, sourceSE
  if not destList or len( sourceSE ) > 1:
    print "No destination SE" if not destList else "More than one source SE"
    Script.showHelp()
  if sourceSE:
    sourceSE = sourceSE[0]

  from DIRAC.Interfaces.API.Dirac                         import Dirac
  dirac = Dirac()

  if args:
    if os.path.isdir( args[-1] ):
      localCache = args.pop()

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )
  if not lfnList:
    print "No LFNs provided..."
    Script.showHelp()

  #print lfnList, destList, sourceSE, localCache
  finalResult = {'OK':True, 'Value':{"Failed":{}, "Successful":{}}}
  for lfn in lfnList:
    for seName in destList:
      result = dirac.replicateFile( lfn, seName, sourceSE, localCache, printOutput = False )
      if not result['OK']:
        finalResult['Value']["Failed"].setdefault( seName, {} ).update( {lfn:result['Message']} )
      else:
        if result['Value']['Failed']:
          finalResult['Value']['Failed'].setdefault( seName, {} ).update( result['Value']['Failed'] )
        if result['Value']['Successful']:
          if result['Value']['Successful'][lfn].get( 'register' ) == 0 and result['Value']['Successful'][lfn].get( 'replicate' ) == 0:
            result['Value']['Successful'][lfn] = 'Already present'
          finalResult['Value']['Successful'].setdefault( seName, {} ).update( result['Value']['Successful'] )

  DIRAC.exit( printDMResult( finalResult ) )
