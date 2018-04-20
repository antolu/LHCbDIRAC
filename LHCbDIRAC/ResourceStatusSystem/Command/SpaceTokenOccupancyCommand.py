""" SpaceTokenOccupancyCommand

  LHCbDIRAC extension adding records to the accounting

"""

__RCSID__ = "$Id$"

from datetime import datetime

# DIRAC
from DIRAC import S_OK
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Command.FreeDiskSpaceCommand import FreeDiskSpaceCommand as FDSC

# LHCbDIRAC
from LHCbDIRAC.AccountingSystem.Client.Types.SpaceToken import SpaceToken


class FreeDiskSpaceCommand(FDSC):
  """ FreeDiskSpaceCommand

  Extension of DIRAC.ResourceStatusSystem.Command.FreeDiskSpaceCommand
  to add entries to accounting. To be considered to move up to DIRAC repository.

  """

  def _storeCommand(self, results):
    """ _storeCommand

    Copy of original method adding records to accounting.

    """

    for result in results:

      resQuery = self.rmClient.addOrModifySpaceTokenOccupancyCache(endpoint=results['Endpoint'],
                                                                   lastCheckTime=datetime.utcnow(),
                                                                   free=results['Free'],
                                                                   total=results['Total'],
                                                                   token=results['ElementName'])
      if not resQuery['OK']:
        return resQuery

      siteRes = DMSHelpers().getSitesForSE(result['Endpoint'])
      if not siteRes['OK']:
        return siteRes

      accountingDict = {
          'SpaceToken': result['Token'],
          'Endpoint': result['Endpoint'],
          'Site': siteRes['Value'][0]
      }

      result['Used'] = result['Total'] - result['Free']

      for sType in ['Total', 'Free', 'Used', 'Guaranteed']:
        spaceTokenAccounting = SpaceToken()
        spaceTokenAccounting.setNowAsStartAndEndTime()
        spaceTokenAccounting.setValuesFromDict(accountingDict)
        spaceTokenAccounting.setValueByKey('SpaceType', sType)
        spaceTokenAccounting.setValueByKey('Space', result[sType] * 1e12)

        gDataStoreClient.addRegister(spaceTokenAccounting)
    gDataStoreClient.commit()

    return S_OK()


#...............................................................................
# EOF
