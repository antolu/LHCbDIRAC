#!/usr/bin/env python

"""  Check the space token usage at the site and report the space usage from several sources:
      File Catalogue, Storage dumps, SRM interface
"""

__RCSID__ = "$Id: dirac-dms-spaceTokens-usage.py 78957 2014-10-31 11:02:44Z fstagni $"

from DIRAC.Core.Base import Script
unit = 'TB'
sites = []
Script.registerSwitch( "u:", "Unit=", "   Unit to use [%s] (MB,GB,TB,PB)" % unit )
Script.registerSwitch( "S:", "Sites=", "  Sites to consider [ALL] (space or comma separated list, e.g. LCG.CERN.ch, LCG.CNAF.it" )
#Script.registerSwitch( "l:", "Site=", "   LCG Site list to check [%s] (e.g. LCG.CERN.ch, LCG.CNAF.it, ... )" %sites )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' %
                                     Script.scriptName, ] ) )


Script.parseCommandLine( ignoreErrors = False )


import DIRAC
from DIRAC.ResourceStatusSystem.Utilities               import CSHelpers
from DIRAC.Core.DISET.RPCClient                         import RPCClient
import time
storageUsage = RPCClient( 'DataManagement/StorageUsage' )

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "u" or switch[0].lower() == "unit":
    unit = switch[1]
  if switch[0] == "S" or switch[0].lower() == "sites":
    sites = switch[1].replace( ',', ' ' ).split()

if not sites:
  sites = ['LCG.CERN.ch' , 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
           'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl', 'LCG.RRCKI.ru']

scaleDict = { 'MB' : 1000 * 1000.0,
              'GB' : 1000 * 1000 * 1000.0,
              'TB' : 1000 * 1000 * 1000 * 1000.0,
              'PB' : 1000 * 1000 * 1000 * 1000 * 1000.0}
if not unit in scaleDict.keys():
  Script.showHelp()
scaleFactor = scaleDict[unit]


spaceTokens = ['LHCb-Tape', 'LHCb-Disk', 'LHCb_USER' ]
SitesSEs = {}
for lcgSite in sites:
  site = lcgSite.split( '.' )[1]
  SitesSEs[site] = {}
  for st in spaceTokens:
    SitesSEs[ site ][ st ] = {}
    SitesSEs[ site ][ st ]['SEs'] = []
  SitesSEs[ site ]['LHCb-Tape']['SEs'] = [site + '-RAW', site + '-RDST', site + '-ARCHIVE']
  SitesSEs[ site ]['LHCb-Disk']['SEs'] = [site + '-BUFFER', site + '-DST', site + '_M-DST', site + '_MC_M-DST', site + '_MC-DST', site + '-FAILOVER']
  SitesSEs[ site ]['LHCb_USER']['SEs'] = [ site + '-USER']
  SitesSEs[ site ]['LHCb-Tape']['type'] = 't1d0'
  SitesSEs[ site ]['LHCb-Disk']['type'] = 't0d1'
  SitesSEs[ site ]['LHCb_USER']['type'] = 't0d1'


try:
  spaceTokenInfo = CSHelpers.getSpaceTokenEndpoints()
except:
  print 'ERROR: could not retrieve space tokens info from the CS'
  DIRAC.exit( -1 )

import lcg_util
def getSrmUsage( lcgSite ):
  """Get space usage via SRM interface
  """
  try:
    site = lcgSite.split( '.' )[ 1 ]
  except:
    print( "Site name is not correct. Should be given in Dirac format: e.g. LCG.CERN.ch" )
    return -1
  if site not in spaceTokenInfo.keys():
    print( "ERROR: information not available for site %s. Space token information from CS: %s " % ( site, spaceTokenInfo ) )
    return -1
  try:
    ep = spaceTokenInfo[ site ]['Endpoint']
  except:
    print 'ERROR! endpoint information not available for site! ' , spaceTokenInfo[ site ]
    return -1

  result = {}
  for st in spaceTokens:
    result[ st ] = {}
    srm = lcg_util.lcg_stmd( st, ep , True, 0 )
    if srm[0]:
      print 'ERROR! for ep = %s and st = %s' % ( ep, st )
      continue
    srmVal = srm[1][0]
    srmTotSpace = srmVal['totalsize']
    # correct for the 6% overhead due to castor setup at RAL
    if 'gridpp' in ep:
      srmTotSpace = ( srmVal['totalsize'] ) * 0.94
      print 'WARNING! apply a 0.94 factor to total space for RAL!'
    srmFree = srmVal['unusedsize']
    srmUsed = srmTotSpace - srmFree
    result[st]['SRMUsed'] = srmUsed
    result[st]['SRMFree'] = srmFree
    result[st]['SRMTotal'] = srmTotSpace
  return result

#.................................................

def getSDUsage( lcgSite ):
  """ get storage usage from storage dumps
  """
  try:
    site = lcgSite.split( '.' )[ 1 ]
  except:
    print( "Site name is not correct. Should be given in Dirac format: e.g. LCG.CERN.ch" )
    return -1
  res = storageUsage.getSTSummary( site )
  if not res['OK']:
    print( "ERROR: Cannot get storage dump information for site %s : %s " % ( site, res['Message'] ) )
    return -1
  sdUsage = {}
  if not res['Value']:
    print( " No information available for site %s from storage dumps" % site )
    return -1
  for row in res['Value']:
    site, spaceTokenWithID, totalSpace, totalFiles, lastUpdate = row
    for st in spaceTokens:
      if st not in sdUsage.keys():
        sdUsage[ st ] = {}
      if st in spaceTokenWithID:
        sdUsage[ st ]['Size'] = totalSpace
        sdUsage[ st ]['Files'] = totalFiles
        sdUsage[ st ]['LastUpdate'] = lastUpdate
        break
  return sdUsage

#......................................................................................

def getLFCUsage( lcgSite ):
  """ get storage usage from LFC
  """
  site = lcgSite.split( '.' )[ 1 ]
  res = storageUsage.getStorageSummary()
  if not res[ 'OK' ]:
    print 'ERROR! ', res
    DIRAC.exit( 1 )
  usage = {}
  for st in spaceTokens:
    usage[ st ] = {'Files':0, 'Size':0}
  for se in res['Value'].keys():
    for st in spaceTokens:
      if se in SitesSEs[ site ][ st ]['SEs']:
        usage[ st ]['Files'] += res['Value'][ se ][ 'Files' ]
        usage[ st ]['Size'] += res['Value'][ se ][ 'Size' ]
        break

  return usage

#......................................................................................


lfcUsage = {}
srmUsage = {}
sdUsage = {}
for site in sites:
  # retrieve space usage from LFC
  lfcUsage[ site ] = getLFCUsage( site )

  # retrieve SRM usage 
  srmResult = getSrmUsage( site )
  if srmResult != -1:
    srmUsage[ site ] = srmResult

  # retrieve space usage from storage dumps:
  sdResult = getSDUsage( site )
  if sdResult != -1:
    sdUsage[ site ] = sdResult

  print( "Storage usage summary for site %s - %s " % ( site, time.asctime() ) )
  for st in spaceTokens:
    print( "Space token %s " % st )
    print( "From LFC: Files: %d, Size: %.2f %s" % ( lfcUsage[ site ][ st ]['Files'], lfcUsage[ site ][ st ]['Size'] / scaleFactor, unit ) )
    if site in srmUsage.keys():
      print( "From SRM: Total Assigned Space: %.2f, Used Space: %.2f, Free Space: %.2f %s " % ( srmUsage[ site ][ st ]['SRMTotal'] / scaleFactor, srmUsage[ site ][ st ]['SRMUsed'] / scaleFactor, srmUsage[ site ][ st ]['SRMFree'] / scaleFactor, unit ) )
    else:
      print( "From SRM: Information not available" )
    if site in sdUsage.keys():
      print( "From storage dumps: Files: %d, Size: %.2f %s - last update %s " % ( sdUsage[ site ][ st ]['Files'], sdUsage[ site ][ st ]['Size'] / scaleFactor, unit, sdUsage[ site ][ st ]['LastUpdate'] ) )
    else:
      print( "From storage dumps: Information not available" )
DIRAC.exit( 0 )

