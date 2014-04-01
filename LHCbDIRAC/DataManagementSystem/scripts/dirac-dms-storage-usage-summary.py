#!/usr/bin/env python
########################################################################
"""
  Get the storage usage summary for the given directories
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC.Core.DISET.RPCClient import RPCClient

seSvcClassDict = {}

def seSvcClass( se ):
  if se not in seSvcClassDict:
    from DIRAC.Resources.Storage.StorageElement import StorageElement
    try:
      if se.endswith( 'HIST' ):
        seSvcClass[se] = 'Hist'
      else:
        seSvcClassDict[se] = 'Tape' if StorageElement( se ).getStatus()['Value']['TapeSE'] else 'Disk'
    except:
      seSvcClassDict[se] = 'LFN'
  return seSvcClassDict[se]

def orderSEs( listSEs ):
  listSEs.sort()
  orderedSEs = ['LFN'] if 'LFN' in listSEs else []
  orderedSEs += sorted( [se for se in listSEs if se not in orderedSEs and se.endswith( '-HIST' )] )
  orderedSEs += sorted( [se for se in listSEs if se not in orderedSEs and ( seSvcClass( se ) == 'Tape' )] )
  orderedSEs += sorted( [se for se in listSEs if se not in orderedSEs] )
  return orderedSEs

def printSEUsage( totalUsage, grandTotal, scaleFactor ):
  """ Nice printout of SE usage """
  line = '-' * 48
  print line
  print '%s %s %s' % ( 'DIRAC SE'.ljust( 20 ), ( 'Size (%s)' % unit ).ljust( 20 ), 'Files'.ljust( 20 ) )
  form = '%.1f'
  for se in orderSEs( totalUsage.keys() ):
    usageDict = totalUsage[se]
    size = usageDict['Size'] / scaleFactor
    if size < 1.:
      form = '%.3f'
      break
  svcClass = ''
  sumFiles = 0
  sumSize = 0.
  for se in orderSEs( totalUsage.keys() ):
    newSvcClass = seSvcClass( se )
    if newSvcClass != svcClass:
      if svcClass:
        print "%s %s %s" % ( ( 'Total (%s)' % svcClass ).ljust( 20 ),
                             ( form % ( sumSize ) ).ljust( 20 ),
                             str( sumFiles ).ljust( 20 ) )
        sumFiles = 0
        sumSize = 0.
      print line
    svcClass = newSvcClass
    usageDict = totalUsage[se]
    files = usageDict['Files']
    size = usageDict['Size'] / scaleFactor
    sumFiles += files
    sumSize += size
    print "%s %s %s" % ( se.ljust( 20 ), ( form % ( size ) ).ljust( 20 ), str( files ).ljust( 20 ) )
  if grandTotal:
    size = grandTotal['Size'] / scaleFactor
    print "%s %s %s" % ( 'Total (disk)'.ljust( 20 ),
                         ( form % ( size ) ).ljust( 20 ),
                         str( grandTotal['Files'] ).ljust( 20 ) )
  print line

def printBigTable( siteList, bigTable ):
  """ Print out THE big table of usage """
  siteList.sort()
  for site in ( 'ARCHIVE', 'LFN' ):
    if site in siteList:
      siteList.remove( site )
      siteList.insert( 0, site )
  just = [20, 30, 15]
  for cond in bigTable:
    just[0] = max( just[0], len( cond ) + 1 )
    for processingPass in sorted( bigTable[cond] ):
      just[1] = max( just[1], len( processingPass ) + 1 )
  prStr = 'Conditions'.ljust( just[0] ) + 'ProcessingPass'.ljust( just[1] )
  for site in siteList:
    prStr += site.ljust( just[2] )
  print prStr
  grandTotal = {}
  for cond in sorted( bigTable ):
    print cond.ljust( just[0] )
    for processingPass in sorted( bigTable[cond] ):
      prStr = ''.ljust( just[0] ) + processingPass.ljust( just[1] )
      bigTableUsage = bigTable[cond][processingPass][1]
      for site in siteList:
        if site in bigTableUsage:
          grandTotal[site] = grandTotal.setdefault( site, 0 ) + bigTableUsage[site]
          prStr += ( '%.3f' % bigTableUsage[site] ).ljust( just[2] )
        elif site == 'Total':
          prStr += '0'.ljust( just[2] )
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
    sys.stdout.write( ' ' * infoStringLength + '\r' )
  sys.stdout.write( str + '\r' )
  sys.stdout.flush()
  infoStringLength = len( str ) + 1

def browseBK( bkQuery, ses, scaleFactor ):
  from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE
  bkPath = bkQuery.getPath()
  if not bkQuery.getConfiguration():
    print "The Configuration should be specified in the --BKQuery option: %s" % bkPath
    return None
  conditions = bkQuery.getBKConditions()
  if not conditions:
    print 'No Conditions found for this Configuration %s' % bkPath
    return None
  requestedEventTypes = bkQuery.getEventTypeList()
  requestedFileTypes = bkQuery.getFileTypeList()
  requestedPP = bkQuery.getProcessingPass()
  requestedConditions = bkQuery.getConditions()
  bigTable = {}
  siteList = []
  for cond in conditions:
    strCond = "Browsing conditions %s" % cond
    writeInfo( strCond )
    bkQuery.setConditions( cond )
    eventTypesForPP = bkQuery.getBKProcessingPasses()
    # print eventTypesForPP
    for processingPass in [pp for pp in eventTypesForPP if eventTypesForPP[pp]]:
      if requestedEventTypes:
        eventTypes = [t for t in requestedEventTypes if t in eventTypesForPP[processingPass]]
        if not eventTypes:
          continue
      else:
        eventTypes = eventTypesForPP[processingPass]
      strPP = " - ProcessingPass %s" % processingPass
      writeInfo( strCond + strPP )
      bkQuery.setProcessingPass( processingPass )
      totalUsage = {}
      grandTotal = {}
      allProds = []
      # print eventTypes
      for eventType in eventTypes:
        strEvtType = " - EventType %s" % str( eventType )
        writeInfo( strCond + strPP + strEvtType )
        bkQuery.setEventType( eventType )
        fileTypes = bkQuery.getBKFileTypes()
        bkQuery.setFileType( fileTypes )
        # print fileTypes
        if not fileTypes or None in fileTypes:
          continue
        prods = bkQuery.getBKProductions()
        prods.sort()
        # print prods
        strProds = " - FileTypes %s - Prods %s" % ( str( fileTypes ), str( prods ) )
        writeInfo( strCond + strPP + strEvtType + strProds )
        info = bkQuery.getNumberOfLFNs()
        nbFiles = info['NumberOfLFNs']
        size = info['LFNSize']
        totalUsage['LFN']['Size'] = totalUsage.setdefault( 'LFN', {} ).setdefault( 'Size', 0 ) + size
        totalUsage['LFN']['Files'] = totalUsage['LFN'].setdefault( 'Files', 0 ) + nbFiles
        if 'DST' not in fileTypes:
          fileTypes.append( 'DST' )
        if 'MDST' not in fileTypes:
          fileTypes.append( 'MDST' )
        if  prods:
          allProds += prods
          for prodID in prods:
            totalUsage, grandTotal = getStorageSummary( totalUsage, grandTotal, '', fileTypes, prodID, ses )
        bkQuery.setFileType( requestedFileTypes )
        bkQuery.setEventType( requestedEventTypes )
      writeInfo( '' )
      if allProds:
        allProds.sort()
        print cond, processingPass, allProds
        printSEUsage( totalUsage, grandTotal, scaleFactor )
        processingPass = processingPass.replace( '/Real Data', '' )
        bigTable.setdefault( cond, {} )[processingPass] = [allProds, {}]
        bigTableUsage = bigTable[cond][processingPass][1]
        for se in totalUsage:
          if se.endswith( '-ARCHIVE' ):
            site = 'ARCHIVE'
          elif se == 'LFN':
            site = 'LFN'
          else:
            res = getSitesForSE( se, gridName = 'LCG' )
            if res['OK'] and len( res['Value'] ) > 0:
              site = res['Value'][0]
            else: continue
          if site not in siteList:
            siteList.append( site )
          bigTableUsage[site] = bigTableUsage.setdefault( site, 0 ) + ( totalUsage[se]['Size'] / scaleFactor )
      bkQuery.setProcessingPass( requestedPP )
    bkQuery.setConditions( requestedConditions )
  import datetime
  print '\n', bkQuery.getPath(), str( datetime.datetime.today() ).split()[0]
  printBigTable( siteList, bigTable )


def getStorageSummary( totalUsage, grandTotal, dirName, fileTypes, prodID, ses ):
  if not totalUsage:
    totalUsage = {}
  if not grandTotal:
    grandTotal = {'Files':0, 'Size':0}
  if type( fileTypes ) != type( [] ):
    fileTypes = [fileTypes]
  for fileType in fileTypes:
    res = RPCClient( 'DataManagement/StorageUsage' ).getStorageSummary( dirName, fileType, prodID, ses )
    if res['OK']:
      for se in res['Value']:
        totalUsage.setdefault( se, {'Files':0, 'Size':0} )
        totalUsage[se]['Files'] += res['Value'][se]['Files']
        totalUsage[se]['Size'] += res['Value'][se]['Size']
        if seSvcClass( se ) == 'Disk' and not se.endswith( '-HIST' ):
          grandTotal['Files'] += res['Value'][se]['Files']
          grandTotal['Size'] += res['Value'][se]['Size']
  return totalUsage, grandTotal

def execute( unit, minimum ):

  from DIRAC import gConfig, gLogger

  gLogger.setLevel( 'FATAL' )
  lcg = False
  full = False
  bkBrowse = False
  users = None
  summary = False
  rank = False
  ses = []
  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() == "u" or switch[0].lower() == "unit":
      unit = switch[1]
    elif switch[0].lower() == "l" or switch[0].lower() == "lcg":
      lcg = True
    elif switch[0].lower() == "full":
      full = True
    elif switch[0] == "BrowseBK":
      bkBrowse = True
    elif switch[0] == "Users":
      if switch[1].lower() == 'all':
        users = sorted( gConfig.getSections( '/Registry/Users' )['Value'] )
        summary = True
        rank = True
      else:
        users = sorted( switch[1].split( ',' ) )
    elif switch[0] == "Summary":
      summary = True
    elif switch[0] == 'Minimum':
      minimum = float( switch[1] )
    elif switch[0] == 'UnknownSE':
      ses = switch[1].split( ',' )

  scaleDict = { 'MB' : 1000 * 1000.0,
                'GB' : 1000 * 1000 * 1000.0,
                'TB' : 1000 * 1000 * 1000 * 1000.0,
                'PB' : 1000 * 1000 * 1000 * 1000 * 1000.0}
  if not unit in scaleDict.keys():
    Script.showHelp()
  scaleFactor = scaleDict[unit]

  ses += dmScript.getOption( 'SEs', [] )
  sites = dmScript.getOption( 'Sites', [] )
  for site in sites:
    res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
    if not res['OK']:
      print 'Site %s not known' % site
      Script.showHelp()
    ses.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )

  # Create a bkQuery looking at all files
  if bkBrowse:
    bkQuery = dmScript.getBKQuery( visible = 'All' )
    browseBK( bkQuery, ses, scaleFactor )
    DIRAC.exit( 0 )

  dirs = dmScript.getOption( 'Directory', [] )
  if users:
    dirs += ['/lhcb/user/%s/%s' % ( user[0], user ) for user in users]
  prods = None
  bkQuery = dmScript.getBKQuery()
  # print bkQuery
  fileTypes = bkQuery.getFileTypeList()
  # print fileTypes
  if type( fileTypes ) != type( [] ):
    fileTypes = [fileTypes]
  if not dirs:
    dirs = ['']
    bkQuery = dmScript.getBKQuery( visible = 'All' )
    bkFileTypes = bkQuery.getFileTypeList()
    if bkFileTypes:
      fileTypes = bkFileTypes
    if bkQuery.getQueryDict().keys() not in ( [], ['Visible'], ['FileType'], ['Visible', 'FileType'] ):
      print "BK query:", bkQuery
      if fileTypes == ['RAW']:
        # For RAW data, get the list of directories...
        dirs = bkQuery.getDirs()
      else:
        prods = sorted( bkQuery.getBKProductions() )
        if not prods:
          print 'No productions found for bkQuery %s' % str( bkQuery )
          DIRAC.exit( 0 )
        # As storageSummary deals with directories and not real file types,
        #    add DST in order to cope with old naming convention
        if fileTypes and 'FULL.DST' not in fileTypes and 'DST' not in fileTypes:
          fileTypes.append( 'DST' )
        print "Looking for %d productions:" % len( prods ), prods
    elif fileTypes and fileTypes[0]:
      print 'FileTypes:', fileTypes

  rpc = RPCClient( 'DataManagement/StorageUsage' )

  if not prods:
    prods = ['']
  if not fileTypes:
    fileTypes = ['']
  prString = "Storage usage for "
  if sites:
    prString += "Sites %s" % str( sites )
  elif ses:
    prString += "SEs %s" % str( ses )
  if prods[0] != '':
    prString += 'productions %s ' % str( prods )
  if fileTypes[0] not in ( '', None ):
    prString += 'file types %s ' % str( fileTypes )
  if dirs[0] != '':
    if fileTypes == ['RAW']:
      prString += 'in %d directories' % len( dirs )
    else:
      prString += 'directories %s' % str( dirs )
  if not users:
    print prString
  if full:
    dirData = {}
    for prodID in sorted( prods ):
      for fileType in fileTypes:
        for dirName in dirs:
          res = rpc.getStorageDirectoryData( dirName, fileType, prodID, ses )
          if not res['OK']:
            print 'Failed to get directories', res['Message']
            DIRAC.exit( 2 )
          dirData.update( res[ 'Value' ] )
    for resDir in sorted( dirData ):
      print resDir, " -> ", dirData[ resDir ]

  totalUsage = None
  grandTotal = None
  usersUsage = {}
  for prodID in prods:
    for fileType in fileTypes:
      for dirName in dirs:
        if users:
          user = dirName.split( '/' )[-1]
          userRegistry = gConfig.getOptions( '/Registry/Users/%s' % user )
          if not userRegistry['OK']:
            quota = 0
          else:
            quota = gConfig.getValue( '/Registry/Users/%s/Quota' % user, 0 )
            if not quota:
              quota = gConfig.getValue( '/Registry/DefaultStorageQuota', 0 )
          quota *= scaleDict['GB'] / scaleFactor
          totalUsage, grandTotal = getStorageSummary( 0, 0, dirName, fileType, prodID, ses )
          spaceUsed = grandTotal['Size'] / scaleFactor
          if summary:
            usersUsage[user] = ( spaceUsed, quota )
          else:
            print "Storage usage for user %s (quota: %.1f %s)%s" % \
                   ( user, quota, unit, ' <== User no longer registered' if not quota else ( ' <== Over quota' if spaceUsed > quota else '' ) )
            printSEUsage( totalUsage, grandTotal, scaleFactor )
        else:
          totalUsage, grandTotal = getStorageSummary( totalUsage, grandTotal, dirName, fileType, prodID, ses )

  if lcg:
    from DIRAC.Resources.Storage.StorageElement                    import StorageElement
    tapeTotalFiles = 0
    diskTotalFiles = 0
    tapeTotalSize = 0
    diskTotalSize = 0
    for se in sorted( totalUsage ):
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
    print '%s %s %s' % ( 'Storage Type'.ljust( 20 ),
                         ( 'Size (%s)' % unit ).ljust( 20 ),
                         'Files'.ljust( 20 ) )
    print '-' * 50
    print "%s %s %s" % ( 'T1D*'.ljust( 20 ),
                         ( '%.1f' % ( tapeTotalSize / scaleFactor ) ).ljust( 20 ),
                         str( tapeTotalFiles ).ljust( 20 ) )
    print "%s %s %s" % ( 'T*D1'.ljust( 20 ),
                         ( '%.1f' % ( diskTotalSize / scaleFactor ) ).ljust( 20 ),
                         str( diskTotalFiles ).ljust( 20 ) )
    DIRAC.exit( 0 )

  if not users:
    printSEUsage( totalUsage, grandTotal, scaleFactor )
  elif summary:
    if rank:
      users = sorted( usersUsage, cmp = ( lambda u1, u2: int( ( usersUsage[u2][0] - usersUsage[u1][0] ) * 1000. ) ) )
    for user in users:
      spaceUsed, quota = usersUsage[user]
      if spaceUsed > minimum:
        print "Storage usage for user %8s: %6.3f %s (quota: %4.1f %s)%s" % \
              ( user, spaceUsed, unit, quota, unit, ' <== User no longer registered' if not quota else ( ' <== Over quota' if spaceUsed > quota else '' ) )

  DIRAC.exit( 0 )

#=====================================================================================
if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerNamespaceSwitches()
  dmScript.registerSiteSwitches()

  unit = 'TB'
  minimum = 0.1
  Script.registerSwitch( "u:", "Unit=", "   Unit to use [%s] (MB,GB,TB,PB)" % unit )
  Script.registerSwitch( "l", "LCG", "  Group results by tape and disk" )
  Script.registerSwitch( '', "Full", "  Output the directories matching selection" )
  Script.registerSwitch( '', "BrowseBK", "   Loop overall paths matching the BK query" )
  Script.registerSwitch( '', "Users=", "   Get storage usage for a (list of) user(s)" )
  Script.registerSwitch( '', 'Summary', '  Only print a summary for users' )
  Script.registerSwitch( '', 'Minimum=', "   Don't print usage for users below that usage (same units, default %.1f" % minimum )
  Script.registerSwitch( '', 'UnknownSE=', "   Force using a non-existing SE name" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )


  Script.parseCommandLine( ignoreErrors = False )

  execute( unit, minimum )
