"""
This class gets information about storage space tokens from the FC recording,
from SRM and if available from storage dumps
It reports for each site its availability and usage
"""

import time

from DIRAC import gLogger, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement

sitesSEs = {}
spaceTokenInfo = {}


def combinedResult(unit, sites=None):
  """ Checks the space token usage at the site and report the space usage from several sources:
      File Catalog, Storage dumps, SRM interface
  """

  dmsHelper = DMSHelpers()

  if not sites:
    # Tier0 and all Tier1s
    sites = sorted(dmsHelper.getTiers(tier=(0, 1)))
  else:
    # Translate in case it is a short name
    allSites = dmsHelper.getSites()
    sites = [dmsHelper.getShortSiteNames(tier=(0, 1)).get(site, site) for site in sites]
    badSites = set(sites) - set(allSites)
    if badSites:
      gLogger.warn("Some sites do not exist", str(sorted(badSites)))
      sites = [site for site in sites if site in allSites]

  scaleDict = {'MB': 1000 * 1000.0,
               'GB': 1000 * 1000 * 1000.0,
               'TB': 1000 * 1000 * 1000 * 1000.0,
               'PB': 1000 * 1000 * 1000 * 1000 * 1000.0}
  if unit not in scaleDict:
    return S_ERROR("Unit not in %s" % scaleDict.keys())
  scaleFactor = scaleDict[unit]

  fcUsage = {}
  srmUsage = {}
  sdUsage = {}

  for site in sites:
    # retrieve space usage from FC
    fcUsage[site] = getFCUsage(site)

    sitesSEs[site] = {}
    # Get SEs at site
    seList = dmsHelper.getSEsAtSite(site).get('Value', [])
    for se in seList:
      occupancyResult = StorageElement(se).getOccupancy()
      if not occupancyResult['OK']:
        return occupancyResult
      occupancy = occupancyResult['Value']
      srmUsage[site] = occupancy

    # retrieve space usage from storage dumps:
    sdResult = getSDUsage(site)
    if sdResult != -1:
      sdUsage[site] = sdResult
    else:
      return 1

    gLogger.notice("Storage usage summary for site %s - %s " % (site.split('.')[1], time.asctime()))
    for st in sitesSEs[site]:
      gLogger.notice("Space token %s " % st)
      gLogger.notice("\tFrom FC: Files: %d, Size: %.2f %s" %
                     (fcUsage[site][st]['Files'],
                      fcUsage[site][st]['Size'] / scaleFactor, unit))
      if site in srmUsage and st in srmUsage[site]:
        gLogger.notice("\tFrom SRM: Total Assigned Space: %.2f %s, Used Space: %.2f %s, Free Space: %.2f %s " %
                       (srmUsage[site][st]['SRMTotal'] / scaleFactor, unit,
                        srmUsage[site][st]['SRMUsed'] / scaleFactor, unit,
                        srmUsage[site][st]['SRMFree'] / scaleFactor, unit))
      else:
        gLogger.notice("\tFrom SRM: Information not available")
      if site in sdUsage and st in sdUsage[site]:
        gLogger.notice("\tFrom storage dumps: Files: %d, Size: %.2f %s - last update %s " %
                       (sdUsage[site][st]['Files'],
                        sdUsage[site][st]['Size'] / scaleFactor, unit,
                        sdUsage[site][st]['LastUpdate']))
      else:
        gLogger.notice("\tFrom storage dumps: Information not available")
  return 0


def getSDUsage(lcgSite):
  """ get storage usage from storage dumps
  """
  try:
    site = lcgSite.split('.')[1]
  except IndexError:
    site = lcgSite
  res = RPCClient('DataManagement/StorageUsage').getSTSummary(site)
  if not res['OK']:
    gLogger.error("ERROR: Cannot get storage dump information for site %s :" % site, res['Message'])
    return -1
  if not res['Value']:
    gLogger.warn(" No information available for site %s from storage dumps" % site)
  sdUsage = {}
  for row in res['Value']:
    site, spaceTokenWithID, totalSpace, totalFiles, lastUpdate = row
    for st in sitesSEs[lcgSite]:
      sdUsage.setdefault(st, {})
      if st in spaceTokenWithID:
        sdUsage[st]['Size'] = totalSpace
        sdUsage[st]['Files'] = totalFiles
        sdUsage[st]['LastUpdate'] = lastUpdate
        break
  return sdUsage


def getFCUsage(lcgSite):
  """ get storage usage from LFC
  """
  res = RPCClient('DataManagement/StorageUsage').getStorageSummary()
  if not res['OK']:
    gLogger.error('ERROR in getStorageSummary ', res['Message'])
    return {}
  storageSummary = res['Value']

  usage = {}
  for st in sitesSEs[lcgSite]:
    usage[st] = {'Files': 0, 'Size': 0}
    for se in sitesSEs[lcgSite][st]['SEs']:
      if se in storageSummary:
        usage[st]['Files'] += storageSummary[se]['Files']
        usage[st]['Size'] += storageSummary[se]['Size']
      else:
        gLogger.error("No FC storage information for SE", se)

  return usage
