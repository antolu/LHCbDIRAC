#!/usr/bin/env python
########################################################################
# File :    dirac-dms-pfn-metadata.py
# Author :  Ph. Charpentier
########################################################################
"""
  Gets the metadata of a (list of) LHCb LFNs/PFNs given a valid DIRAC SE.
  Only the LFN contained in the PFN is considered, unlike the DIRAC similar script
"""
__RCSID__ = "$Id$"
import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
from DIRAC.Core.Base import Script

def __checkSEs( args ):
  expanded = []
  for arg in args:
    expanded += arg.split( ',' )
  seList = []
  res = gConfig.getSections( '/Resources/StorageElements' )
  if res['OK']:
    for ses in list( expanded ):
      sel = [se for se in ses.split( ',' ) if se in res['Value']]
      if sel :
        seList.append( ','.join( sel ) )
        expanded.remove( ses )
  return seList, expanded

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.registerSwitch( '', 'Check', '   Checks the PFN metadata vs LFN metadata' )
  Script.registerSwitch( '', 'Exists', '   Only reports if the file exists' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [URL[,URL2[,URL3...]]] SE[ SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  URL:      Logical/Physical File Name or file containing URLs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine( ignoreErrors = True )
  check = False
  exists = False
  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'Check':
      check = True
    if opt == 'Exists':
      exists = True


  seList = dmScript.getOption( 'SEs', [] )

  from DIRAC import gConfig, gLogger
  if not seList:
    sites = dmScript.getOption( 'Sites', [] )
    for site in sites:
      res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
      if not res['OK']:
        gLogger.fatal( 'Site %s not known' % site )
        Script.showHelp()
      seList.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )
  args = Script.getPositionalArgs()
  if not seList:
    seList, args = __checkSEs( args )
  # This should be improved, with disk SEs first...
  seList.sort()

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  urlList = dmScript.getOption( 'LFNs', [] )
  if not urlList:
    Script.showHelp()
    DIRAC.exit( 0 )

  from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
  from DIRAC import S_OK, S_ERROR
  from DIRAC.Core.Utilities.Adler import compareAdler
  if len( seList ) > 1:
    gLogger.always( "Using the following list of SEs: %s" % str( seList ) )
  rm = ReplicaManager()
  gLogger.setLevel( "FATAL" )
  metadata = {'Successful':{}, 'Failed':{}}
  replicas = {}
  # restrict SEs to those where the replicas are
  res = rm.getCatalogReplicas( urlList, allStatus = True )
  if not res['OK']:
    gLogger.fatal( 'Error getting replicas for %d files' % len( urlList ), res['Message'] )
    DIRAC.exit( 2 )
  else:
    replicas = res['Value']['Successful']
    for lfn in sorted( replicas ):
      if seList and not [se for se in replicas[lfn] if se in seList]:
        metadata['Failed'][lfn] = 'No such file at %s in FC' % ' '.join( seList )
        replicas.pop( lfn )
        urlList.remove( lfn )
    for lfn in res['Value']['Failed']:
      metadata['Failed'][lfn] = 'FC: ' + res['Value']['Failed'][lfn]
    metadata['Failed'].update( dict.fromkeys( [url for url in urlList if url not in replicas and url not in metadata['Failed']], 'No active replicas' ) )
  result = None
  if not seList:
    # take all SEs in replicas and add a fake '' to printout the SE name
    seList = [''] + sorted( list( set( [se for lfn in replicas for se in replicas[lfn]] ) ) )
  if replicas:
    for se in seList:
      fileList = [url for url in urlList if se in replicas.get( url, [] )]
      if not fileList:
        continue
      res = rm.getStorageFileMetadata( fileList, se )
      if res['OK']:
        seMetadata = res['Value']
        for url in seMetadata['Successful']:
          pfnMetadata = seMetadata['Successful'][url].copy()
          metadata['Successful'].setdefault( url, {} )[se] = pfnMetadata if not exists else {'Exists': 'True (%sCached)' % ( '' if pfnMetadata.get( 'Cached' ) else 'Not ' )}
          if exists and not pfnMetadata.get( 'Size' ):
            metadata['Successful'][url][se].update( {'Exists':'Zero size'} )
          if check:
            res1 = rm.getCatalogFileMetadata( url )
            if res1['OK']:
              lfnMetadata = res1['Value']['Successful'][url]
              ok = True
              diff = 'False -'
              for field in ( 'Checksum', 'Size' ):
                if lfnMetadata[field] != pfnMetadata[field]:
                  if field == 'Checksum' and compareAdler( lfnMetadata[field], pfnMetadata[field] ):
                    continue
                  ok = False
                  diff += ' %s: (LFN %s, PFN %s)' % ( field, lfnMetadata[field], pfnMetadata[field] )
              if len( seList ) > 1:
                metadata['Successful'][url][se]['MatchLFN'] = ok if ok else diff
              else:
                metadata['Successful'][url]['MatchLFN'] = ok if ok else diff
          # metadata['Failed'].pop( url, None )
        for url in seMetadata['Failed']:
           # if url not in metadata['Successful']:
           metadata['Failed'].setdefault( url, {} )[se] = seMetadata['Failed'][url] if not exists else {'Exists': False}
        # urlList = seMetadata['Failed']
        # if not urlList: break
      else:
        for url in fileList:
          metadata['Failed'][url] = res['Message'] + ' at %s' % se

  printDMResult( S_OK( metadata ), empty = "File not at SE" )
  DIRAC.exit( 0 )
