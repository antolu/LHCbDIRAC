#!/usr/bin/env python
########################################################################
# File :    dirac-dms-pfn-metadata.py
# Author :  Ph. Charpentier
########################################################################
"""
  Gets the metadata of a (list of) LFNs/PFNs given a valid DIRAC SE.
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
  Script.registerSwitch( '', 'Check', '   Checks the PFN metadata vs LFN metadata' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [URL[,URL2[,URL3...]]] SE[,SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  URL:      Logical/Physical File Name or file containing URLs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine( ignoreErrors = True )
  check = False
  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'Check':
      check = True


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
  urlList = dmScript.getOption( 'LFNs', [] )
  if not urlList:
    Script.showHelp()
    DIRAC.exit( 0 )

  from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
  from DIRAC import gLogger
  if len( seList ) > 1:
    gLogger.info( "Using the following list of SEs: %s" % str( seList ) )
  rm = ReplicaManager()
  gLogger.setLevel( "FATAL" )
  result = {'OK':True, 'Value': {'Successful':{}, 'Failed':{}}}
  for se in seList:
    res = rm.getStorageFileMetadata( urlList, se )
    if res['OK']:
      result['Value']['Successful'].update( res['Value']['Successful'] )
      result['Value']['Failed'].update( res['Value']['Failed'] )
      for url in res['Value']['Successful']:
        if check:
          pfnMetadata = res['Value']['Successful'][url]
          res1 = rm.getCatalogFileMetadata( url )
          if res1['OK']:
            lfnMetadata = res1['Value']['Successful'][url]
            ok = True
            for field in ( 'Checksum', 'Size' ):
              if lfnMetadata[field] != pfnMetadata[field]:
                ok = False
            result['Value']['Successful'][url][' MatchLFN'] = ok
        result['Value']['Failed'].pop( url, None )
      for url in res['Value']['Failed']:
        result['Value']['Failed'][url] += ' at %s' % ', '.join( seList )
      urlList = res['Value']['Failed']
      if not urlList: break
    else:
      result['OK'] = False

  if result['OK']:
    printDMResult( result, empty = "File not at SE" )
  else:
    gLogger.fatal( "Error getting metadata for %s at %s" % ( urlList, str( seList ) ) )
    printDMResult( res, empty = "File not at SE", script = "dirac-dms-pfn-metadata" )
  DIRAC.exit( 0 )
