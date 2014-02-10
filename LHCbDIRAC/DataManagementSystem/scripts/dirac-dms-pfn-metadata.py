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
  Script.registerSwitch( '', 'Exists', '   Only reports if the file exists (and checks the checksum)' )
  Script.registerSwitch( '', 'Summary', '   Only prints a summary on existing files' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [URL[,URL2[,URL3...]]] SE[ SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  URL:      Logical/Physical File Name or file containing URLs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine( ignoreErrors = True )
  check = False
  exists = False
  summary = False
  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'Check':
      check = True
    elif opt == 'Exists':
      exists = True
      check = True
    elif opt == 'Summary':
      summary = True
      check = True
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

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.Resources.Storage.StorageElement import StorageElement
  from DIRAC import S_OK, S_ERROR
  from DIRAC.Core.Utilities.Adler import compareAdler
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  if len( seList ) > 1:
    gLogger.always( "Using the following list of SEs: %s" % str( seList ) )
  if len( urlList ) > 100:
    gLogger.always( "Getting metadata for %d files, be patient" % len( urlList ) )

  fc = FileCatalog()

  gLogger.setLevel( "FATAL" )
  metadata = {'Successful':{}, 'Failed':{}}
  replicas = {}
  # restrict SEs to those where the replicas are
  for lfnChunk in breakListIntoChunks( urlList, 100 ):
    res = fc.getReplicas( lfnChunk, allStatus = True )
    if not res['OK']:
      gLogger.fatal( 'Error getting replicas for %d files' % len( lfnChunk ), res['Message'] )
      DIRAC.exit( 2 )
    else:
      replicas.update( res['Value']['Successful'] )
    for lfn in res['Value']['Failed']:
      metadata['Failed'][lfn] = 'FC: ' + res['Value']['Failed'][lfn]
  for lfn in sorted( replicas ):
    if seList and not [se for se in replicas[lfn] if se in seList]:
      metadata['Failed'][lfn] = 'No such file at %s in FC' % ' '.join( seList )
      replicas.pop( lfn )
      urlList.remove( lfn )
  metadata['Failed'].update( dict.fromkeys( [url for url in urlList if url not in replicas and url not in metadata['Failed']], 'FC: No active replicas' ) )
  result = None
  if not seList:
    # take all SEs in replicas and add a fake '' to printout the SE name
    seList = [''] + sorted( list( set( [se for lfn in replicas for se in replicas[lfn]] ) ) )
  if replicas:
    for se in seList:
      fileList = [url for url in urlList if se in replicas.get( url, [] )]
      if not fileList:
        continue
      oSe = StorageElement( se )
      for fileChunk in breakListIntoChunks( fileList, 100 ):
        res = oSe.getFileMetadata( fileChunk )
        if res['OK']:
          seMetadata = res['Value']
          for url in seMetadata['Successful']:
            pfnMetadata = seMetadata['Successful'][url].copy()
            metadata['Successful'].setdefault( url, {} )[se] = pfnMetadata if not exists else {'Exists': 'True (%sCached)' % ( '' if pfnMetadata.get( 'Cached' ) else 'Not ' )}
            if exists and not pfnMetadata.get( 'Size' ):
              metadata['Successful'][url][se].update( {'Exists':'Zero size'} )
            if check:
              res1 = fc.getFileMetadata( url )
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
          for url in seMetadata['Failed']:
             metadata['Failed'].setdefault( url, {} )[se] = seMetadata['Failed'][url] if not exists else {'Exists': False}
        else:
          for url in fileChunk:
            metadata['Failed'][url] = res['Message'] + ' at %s' % se

  if not summary:
    printDMResult( S_OK( metadata ), empty = "File not at SE" )
  else:
    nFiles = 0
    counterKeys = ['Not in FC', 'No active replicas', 'Not existing', 'Exists', 'Checksum OK', 'Checksum bad']
    counters = dict.fromkeys( counterKeys, 0 )
    for lfn, reason in metadata['Failed'].items():
      nFiles += 1
      if type( reason ) == type( '' ):
        if reason == 'FC: No active replicas':
          counters['No active replicas'] += 1
        elif reason.startswith( 'FC:' ):
          counters['Not in FC'] += 1
        else:
          counters['Not existing'] += 1
      elif type( reason ) == type( {} ):
        for se in reason:
          if reason[se]['Exists']:
            counters['Exists'] += 1
          else:
            counters['Not existing'] += 1
    for lfn, seDict in metadata['Successful'].items():
      nFiles += 1
      for se in seDict:
        if seDict[se]['MatchLFN'] == True:
          counters['Checksum OK'] += 1
        else:
          counters['Checksum bad'] += 1
    print 'For %d files:' % nFiles
    for key in counterKeys:
      if counters[key]:
        print '%s: %d' % ( key.rjust( 20 ), counters[key] )

  DIRAC.exit( 0 )
