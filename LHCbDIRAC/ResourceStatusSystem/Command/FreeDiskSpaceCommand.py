""" SpaceTokenOccupancyCommand

  LHCbDIRAC extension adding records to the accounting

"""

from datetime import datetime

# DIRAC
from DIRAC import S_OK
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Command.FreeDiskSpaceCommand import FreeDiskSpaceCommand as FDSC
from DIRAC.Resources.Storage.StorageElement import StorageElement

# LHCbDIRAC
from LHCbDIRAC.AccountingSystem.Client.Types.SpaceToken import SpaceToken


__RCSID__ = "$Id$"


class FreeDiskSpaceCommand(FDSC):
  """ FreeDiskSpaceCommand

  Extension of DIRAC.ResourceStatusSystem.Command.FreeDiskSpaceCommand
  to add entries to accounting. To be considered to move up to DIRAC repository.

  """

  def _storeCommand(self, results):
    """ _storeCommand

    Copy of original method adding records to accounting.

    """

    resQuery = self.rmClient.addOrModifySpaceTokenOccupancyCache(endpoint=results['Endpoint'],
                                                                 lastCheckTime=datetime.utcnow(),
                                                                 free=results['Free'],
                                                                 total=results['Total'],
                                                                 token=results['ElementName'])
    if not resQuery['OK']:
      return resQuery

    siteRes = DMSHelpers().getSitesForSE(results['Endpoint'])
    if not siteRes['OK']:
      return siteRes

    se = StorageElement(results['Endpoint'])
    pluginsRes = se.getPlugins()
    if not pluginsRes['OK']:
      return pluginsRes
    tokenRes = se.getStorageParameters(pluginsRes['Value'][0])  # any should be OK for this purpose
    if not tokenRes['OK']:
      return tokenRes

    accountingDict = {
        'SpaceToken': tokenRes['Value']['SpaceToken'],
        'Endpoint': results['Endpoint'],
        'Site': siteRes['Value'][0]
    }

    results['Used'] = results['Total'] - results['Free']

    for sType in ['Total', 'Free', 'Used']:
      spaceTokenAccounting = SpaceToken()
      spaceTokenAccounting.setNowAsStartAndEndTime()
      spaceTokenAccounting.setValuesFromDict(accountingDict)
      spaceTokenAccounting.setValueByKey('SpaceType', sType)
      spaceTokenAccounting.setValueByKey('Space', results[sType] * 1e12)

      gDataStoreClient.addRegister(spaceTokenAccounting)
    gDataStoreClient.commit()

    return S_OK()


#...............................................................................
# EOF
