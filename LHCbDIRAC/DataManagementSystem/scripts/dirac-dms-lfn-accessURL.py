#!/usr/bin/env python

"""
  Retrieve an access URL for an LFN replica given a valid DIRAC SE.
"""

__RCSID__ = "$Id$"


import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
from DIRAC.Core.Base import Script

def __checkSEs( args ):
  """
  Check which arguments are SE names listed in the CS
  """
  seList = []
  from DIRAC import gConfig
  res = gConfig.getSections( '/Resources/StorageElements' )
  if res['OK']:
    for ses in list( args ):
      sel = [se for se in ses.split( ',' ) if se in res['Value']]
      if sel :
        seList += sel
        args.remove( ses )
  return seList, args

def execute():
  """
  Actual script executor
  """

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
  if not lfnList:
    Script.showHelp()
    DIRAC.exit( 0 )

  from DIRAC.DataManagementSystem.Client.DataManager         import DataManager
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  from DIRAC import gLogger
  if len( seList ) > 1:
    gLogger.info( "Using the following list of SEs: %s" % str( seList ) )
  dm = DataManager()
  bk = BookkeepingClient()
  # gLogger.setLevel( "FATAL" )
  notFoundLfns = set( lfnList )
  results = {'OK':True, 'Value':{'Successful':{}, 'Failed':{}}}
  level = gLogger.getLevel()
  gLogger.setLevel( 'FATAL' )
  for se in seList:
    res = dm.getReplicaAccessUrl( lfnList, se )
    success = res.get( 'Value', {} ).get( 'Successful' )
    if res['OK'] and success:
      bkRes = bk.getFileTypeVersion( success.keys() )
      if bkRes['OK']:
        for lfn in bkRes['Value']:
          if bkRes['Value'][lfn] == 'MDF':
            success[lfn] = 'mdf:' + success[lfn]
      notFoundLfns -= set( success )
      results['Value']['Successful'].setdefault( se, {} ).update( success )
  gLogger.setLevel( level )

  if notFoundLfns:
    results['Value']['Failed'] = dict.fromkeys( sorted( notFoundLfns ), 'File not found in required SEs' )

  printDMResult( results, empty = "File not at SE", script = "dirac-dms-lfn-accessURL" )

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.setUsageMessage( '\n'.join( __doc__.split( '\n' ) + [
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]] SE[,SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name or file containing LFNs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  execute()
  DIRAC.exit( 0 )

