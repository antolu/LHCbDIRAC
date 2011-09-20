#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
  Get the storage usage summary for the given directories
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

def orderSEs( listSEs ):
  listSEs = sortList( listSEs )
  orderedSEs = [se for se in listSEs if se == 'LFN']
  orderedSEs += [se for se in listSEs if se.endswith( '-HIST' )]
  orderedSEs += [se for se in listSEs if se.endswith( "-ARCHIVE" ) and se not in orderedSEs]
  orderedSEs += [se for se in listSEs if se not in orderedSEs]
  return orderedSEs

def printSEUsage( totalUsage, grandTotal, scaleFactor ):
  line = '-' * 48
  print line
  print '%s %s %s' % ( 'DIRAC SE'.ljust( 20 ), ( 'Size (%s)' % unit ).ljust( 20 ), 'Files'.ljust( 20 ) )
  print line
  format = '%.1f'
  for se in orderSEs( totalUsage.keys() ):
    dict = totalUsage[se]
    size = dict['Size'] / scaleFactor
    if size < 1.:
      format = '%.3f'
      break
  suffix = ''
  for se in orderSEs( totalUsage.keys() ):
    newSuffix = se.split( '-' )
    newSuffix = newSuffix[-1]
    if newSuffix != suffix and suffix in ( 'LFN', 'HIST', 'ARCHIVE' ):
      print line
    suffix = newSuffix
    dict = totalUsage[se]
    files = dict['Files']
    size = dict['Size'] / scaleFactor
    print "%s %s %s" % ( se.ljust( 20 ), ( format % ( size ) ).ljust( 20 ), str( files ).ljust( 20 ) )
  if grandTotal:
    size = grandTotal['Size'] / scaleFactor
    print "%s %s %s" % ( 'Total (disk)'.ljust( 20 ), ( format % ( size ) ).ljust( 20 ), str( grandTotal['Files'] ).ljust( 20 ) )
  print line

def printBigTable( siteList, bigTable ):
  siteList.sort()
  if 'LFN' in siteList:
    siteList.remove( 'LFN' )
    siteList = ['LFN'] + siteList
  just = [20, 30, 15]
  for cond in bigTable:
    just[0] = max( just[0], len( cond ) + 1 )
    for processingPass in sortList( bigTable[cond].keys() ):
      just[1] = max( just[1], len( processingPass ) + 1 )
  prStr = 'Conditions'.ljust( just[0] ) + 'ProcessingPass'.ljust( just[1] )
  for site in siteList:
    prStr += site.ljust( just[2] )
  print prStr
  grandTotal = {}
  for cond in sortList( bigTable.keys() ):
    print cond.ljust( just[0] )
    for processingPass in sortList( bigTable[cond].keys() ):
      prStr = ''.ljust( just[0] ) + processingPass.ljust( just[1] )
      bigTableUsage = bigTable[cond][processingPass][1]
      for site in siteList:
        if site in bigTableUsage:
          grandTotal[site] = grandTotal.setdefault( site, 0 ) + bigTableUsage[site]
          prStr += ( '%.3f' % bigTableUsage[site] ).ljust( just[2] )
        else:
          prStr += '-'.ljust( just[2] )
      print prStr
  prStr = '\n' + ''.ljust( just[0] ) + 'Grand-Total'.ljust( just[1] )
  for site in siteList:
    if site in grandTotal:
      prStr += ( '%.3f' % grandTotal[site] ).ljust( just[2] )
    else:
      prStr += '-'.ljust( just[2] )
  print prStr

infoStringLength = 1
def writeInfo( str ):
  global infoStringLength
  import sys
  if len( str ) < 1 or str[0] != '\n':
    sys.stdout.write( ' '*infoStringLength + '\r' )
  sys.stdout.write( str + '\r' )
  sys.stdout.flush()
  infoStringLength = len( str ) + 1

def browseBK( dmScript, scaleFactor ):
  from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE
  sites = dmScript.getOption( 'Sites', [] )
  ses = dmScript.getOption( 'SEs', [] )
  bkQuery = dmScript.buildBKQuery()
  mandatoryFields = [ "ConfigName", "ConfigVersion" ]
  for field in mandatoryFields:
    if field not in bkQuery:
      print "BK query should at least contain the configuration..."
      return None
  conditions = dmScript.getBKConditions( bkQuery )
  if not conditions:
    return None
  query = bkQuery.copy()
  if 'Visible' in query:
    query.pop( 'Visible' )
  bigTable = {}
  siteList = []
  for cond in conditions:
    strCond = "Browsing conditions %s" % cond
    writeInfo( strCond )
    query = dmScript.setBKConditions( query, cond )
    processingPasses = dmScript.getBKProcessingPasses( query )
    #print processingPasses
    for processingPass in [pp for pp in processingPasses if processingPasses[pp]]:
      if bkQuery.get( 'EventTypeId' ):
        eventTypes = bkQuery['EventTypeId']
        if type( eventTypes ) != type( [] ):
          eventTypes = [eventTypes]
        eventTypes = [t for t in eventTypes if t in processingPasses[processingPass]]
        if not eventTypes: continue
      else:
        eventTypes = processingPasses[processingPass]
      strPP = " - ProcessingPass %s" % processingPass
      writeInfo( strCond + strPP )
      query['ProcessingPass'] = processingPass
      totalUsage = {}
      grandTotal = {}
      allProds = []
      #print eventTypes
      for eventType in eventTypes:
        strEvtType = " - EventType %s" % str( eventType )
        writeInfo( strCond + strPP + strEvtType )
        query['EventType'] = eventType
        fileTypes = dmScript.getBKFileTypes( query )
        #print fileTypes
        if None in fileTypes: continue
        query = dmScript.setBKFileType( query, fileTypes )
        prods = dmScript.getProductionsFromBKQuery( query, visible = False )
        #print prods
        nbFiles, size = dmScript.getNumberOfFiles( query, visible = False )
        totalUsage['LFN']['Size'] = totalUsage.setdefault( 'LFN', {} ).setdefault( 'Size', 0 ) + size
        totalUsage['LFN']['Files'] = totalUsage['LFN'].setdefault( 'Files', 0 ) + nbFiles
        strProds = " - FileTypes %s - Prods %s" % ( str( fileTypes ), str( prods ) )
        writeInfo( strCond + strPP + strEvtType + strProds )
        if  prods:
          allProds += prods
          for prodID in prods:
            totalUsage, grandTotal = getStorageSummary( totalUsage, grandTotal, '', fileTypes, prodID, ses )
        query = dmScript.setBKFileType( query, bkQuery.get( 'FileType', bkQuery.get( 'FileTypeId' ) ) )
        if 'EventType' not in bkQuery:
          query.pop( 'EventType' )
      writeInfo( '' )
      if allProds:
        allProds.sort()
        print cond, processingPass, allProds
        printSEUsage( totalUsage, grandTotal, scaleFactor )
        processingPass = processingPass.replace( '/Real Data', '' )
        bigTable.setdefault( cond, {} )[processingPass] = [allProds, {}]
        bigTableUsage = bigTable[cond][processingPass][1]
        for se in totalUsage:
          if se.endswith( '-ARCHIVE' ): continue
          if se == 'LFN':
            site = 'LFN'
          else:
            res = getSitesForSE( se, gridName = 'LCG' )
            if res['OK'] and len( res['Value'] ) > 0:
              site = res['Value'][0]
            else: continue
          if site not in siteList:
            siteList.append( site )
          bigTableUsage[site] = bigTableUsage.setdefault( site, 0 ) + ( totalUsage[se]['Size'] / scaleFactor )
      if 'ProcessingPass' not in bkQuery:
        query.pop( 'ProcessingPass' )
      else:
        query['ProcessingPass'] = bkQuery['ProcessingPass']
    query = dmScript.setBKConditions( query, None )
  import datetime
  print '\n', dmScript.getOption( 'BKQuery' ), str( datetime.datetime.today() ).split()[0]
  printBigTable( siteList, bigTable )


def getStorageSummary( totalUsage, grandTotal, dir, fileTypes, prodID, ses ):
  if not totalUsage:
    totalUsage = {}
  if not grandTotal:
    grandTotal = {'Files':0, 'Size':0}
  if type( fileTypes ) != type( [] ):
    fileTypes = [fileTypes]
  # As storageSummary deals with directories and not real file types, add DST in order to cope with old naming convention
  if 'DST' not in fileTypes:
    fileTypes.append( 'DST' )
  for fileType in fileTypes:
    res = RPCClient( 'DataManagement/StorageUsage' ).getStorageSummary( dir, fileType, prodID, ses )
    if res['OK']:
      for se in res['Value']:
        totalUsage.setdefault( se, {'Files':0, 'Size':0} )
        totalUsage[se]['Files'] += res['Value'][se]['Files']
        totalUsage[se]['Size'] += res['Value'][se]['Size']
        if not se.endswith( '-ARCHIVE' ) and not se.endswith( '-HIST' ):
          grandTotal['Files'] += res['Value'][se]['Files']
          grandTotal['Size'] += res['Value'][se]['Size']
  return totalUsage, grandTotal

#=====================================================================================
if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerNamespaceSwitches()
  dmScript.registerSiteSwitches()

  unit = 'TB'
  Script.registerSwitch( "u:", "Unit=", "   Unit to use [%s] (MB,GB,TB,PB)" % unit )
  Script.registerSwitch( "l", "LCG", "  Group results by tape and disk" )
  Script.registerSwitch( '', "Full", "  Output the directories matching selection" )
  Script.registerSwitch( '', "BrowseBK", "   Loop overall paths matching the BK query" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )


  Script.parseCommandLine( ignoreErrors = False )

  from DIRAC import gConfig
  from DIRAC.Core.DISET.RPCClient import RPCClient
  from DIRAC.Core.Utilities.List import sortList

  lcg = False
  full = False
  bkBrowse = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() == "u" or switch[0].lower() == "unit":
      unit = switch[1]
    if switch[0].lower() == "l" or switch[0].lower() == "lcg":
      lcg = True
    if switch[0].lower() == "full":
      full = True
    if switch[0] == "BrowseBK":
      bkBrowse = True

  scaleDict = { 'MB' : 1000 * 1000.0,
                'GB' : 1000 * 1000 * 1000.0,
                'TB' : 1000 * 1000 * 1000 * 1000.0,
                'PB' : 1000 * 1000 * 1000 * 1000 * 1000.0}
  if not unit in scaleDict.keys():
    Script.showHelp()
  scaleFactor = scaleDict[unit]

  if bkBrowse:
    browseBK( dmScript, scaleFactor )
    DIRAC.exit( 0 )

  sites = dmScript.getOption( 'Sites', [] )
  ses = dmScript.getOption( 'SEs', [] )
  prods = dmScript.getOption( 'Productions', [] )
  dirs = dmScript.getOption( 'Directory', [''] )
  bkQuery = dmScript.buildBKQuery()
  if bkQuery and not prods:
    prods = dmScript.getProductionsFromBKQuery( visible = False )
    if not prods:
      print 'No productions found for bkQuery %s' % str( bkQuery )
      DIRAC.exit( 0 )
    prods.sort()
    print "Looking for %d productions:" % len( prods ), prods
  fileTypes = dmScript.getOption( 'FileType' )
  if bkQuery and not fileTypes:
    fileTypes = bkQuery.get( 'FileType', None )
    if type( fileTypes ) == type( '' ):
      fileTypes = [fileTypes]
    if fileTypes:
      print 'FileTypes:', fileTypes
  if not fileTypes:
    fileTypes = ['']
  for site in sites:
    res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
    if not res['OK']:
      print 'Site %s not known' % site
      Script.showHelp()
    ses.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )

  rpc = RPCClient( 'DataManagement/StorageUsage' )
  if not prods:
    prods = ['']

  if full:
    dirData = {}
    for prodID in sortList( prods ):
      for fileType in fileTypes:
        for dir in dirs:
          res = rpc.getStorageDirectoryData( dir, fileType, prodID, ses )
          if not res['OK']:
            print 'Failed to get directories', res['Message']
            DIRAC.exit( 2 )
            dirData.update( res[ 'Value' ] )
    for resDir in sorted( dirData ):
      print resDir, " -> ", dirData[ resDir ]

  totalUsage = None
  grandTotal = None
  for prodID in prods:
    for fileType in fileTypes:
      for dir in dirs:
        totalUsage, grandTotal = getStorageSummary( totalUsage, grandTotal, dir, fileType, prodID, ses )

  if lcg:
    from DIRAC.Resources.Storage.StorageElement                    import StorageElement
    tapeTotalFiles = 0
    diskTotalFiles = 0
    tapeTotalSize = 0
    diskTotalSize = 0
    for se in sortList( totalUsage.keys() ):
      storageElement = StorageElement( se )
      files = totalUsage[se]['Files']
      size = totalUsage[se]['Size']
      seStatus = storageElement.getStatus()['Value']
      if seStatus['TapeSE']:
        tapeTotalFiles += files
        tapeTotalSize += size
      if seStatus['DiskSE']:
        diskTotalFiles += files
        diskTotalSize += size
    print '%s %s %s' % ( 'Storage Type'.ljust( 20 ), ( 'Size (%s)' % unit ).ljust( 20 ), 'Files'.ljust( 20 ) )
    print '-' * 50
    print "%s %s %s" % ( 'T1D*'.ljust( 20 ), ( '%.1f' % ( tapeTotalSize / scaleFactor ) ).ljust( 20 ), str( tapeTotalFiles ).ljust( 20 ) )
    print "%s %s %s" % ( 'T*D1'.ljust( 20 ), ( '%.1f' % ( diskTotalSize / scaleFactor ) ).ljust( 20 ), str( diskTotalFiles ).ljust( 20 ) )
    DIRAC.exit( 0 )

  printSEUsage( totalUsage, grandTotal, scaleFactor )
  DIRAC.exit( 0 )

