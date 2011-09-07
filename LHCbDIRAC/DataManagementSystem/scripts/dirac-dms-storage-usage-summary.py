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
  orderedSEs = [se for se in listSEs if se.endswith( "-ARCHIVE" )]
  orderedSEs += [se for se in listSEs if not se.endswith( "-ARCHIVE" )]
  return orderedSEs

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerNamespaceSwitches()
  dmScript.registerSiteSwitches()

  unit = 'TB'
  dirs = ['']
  fileTypes = None
  prods = []
  sites = []
  ses = []
  lcg = False
  full = False
  Script.registerSwitch( "u:", "Unit=", "   Unit to use [%s] (MB,GB,TB,PB)" % unit )
  Script.registerSwitch( "l", "LCG", "  Group results by tape and disk" )
  Script.registerSwitch( '', "Full", "  Output the directories matching selection" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )


  Script.parseCommandLine( ignoreErrors = False )

  from DIRAC import gConfig
  from DIRAC.Core.DISET.RPCClient import RPCClient
  from DIRAC.Core.Utilities.List import sortList

  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() == "u" or switch[0].lower() == "unit":
      unit = switch[1]
    if switch[0].lower() == "l" or switch[0].lower() == "lcg":
      lcg = True
    if switch[0].lower() == "full":
      full = True

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
    print 'FileTypes:', fileTypes
  if not fileTypes:
    fileTypes = ['']
  for site in sites:
    res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
    if not res['OK']:
      print 'Site %s not known' % site
      Script.showHelp()
    ses.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )
  scaleDict = { 'MB' : 1000 * 1000.0,
                'GB' : 1000 * 1000 * 1000.0,
                'TB' : 1000 * 1000 * 1000 * 1000.0,
                'PB' : 1000 * 1000 * 1000 * 1000 * 1000.0}
  if not unit in scaleDict.keys():
    Script.showHelp()
  scaleFactor = scaleDict[unit]

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

  totalUsage = {}
  grandTotal = {'Files':0, 'Size':0}
  for prodID in prods:
    for fileType in fileTypes:
      for dir in dirs:
        res = rpc.getStorageSummary( dir, fileType, prodID, ses )
        if res['OK']:
          for se in res['Value']:
            if se not in totalUsage:
              totalUsage[se] = {'Files':0, 'Size':0}
            totalUsage[se]['Files'] += res['Value'][se]['Files']
            totalUsage[se]['Size'] += res['Value'][se]['Size']
            if not se.endswith( '-ARCHIVE' ):
              grandTotal['Files'] += res['Value'][se]['Files']
              grandTotal['Size'] += res['Value'][se]['Size']

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

  print '%s %s %s' % ( 'DIRAC SE'.ljust( 20 ), ( 'Size (%s)' % unit ).ljust( 20 ), 'Files'.ljust( 20 ) )
  print '-' * 50
  for se in orderSEs( totalUsage.keys() ):
    dict = totalUsage[se]
    files = dict['Files']
    size = dict['Size']
    print "%s %s %s" % ( se.ljust( 20 ), ( '%.1f' % ( size / scaleFactor ) ).ljust( 20 ), str( files ).ljust( 20 ) )
  print "\n%s %s %s" % ( 'Total (disk)'.ljust( 20 ), ( '%.1f' % ( grandTotal['Size'] / scaleFactor ) ).ljust( 20 ), str( grandTotal['Files'] ).ljust( 20 ) )
  DIRAC.exit( 0 )
