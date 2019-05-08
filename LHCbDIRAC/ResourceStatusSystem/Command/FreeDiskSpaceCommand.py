""" FreeDiskSpaceCommand

  LHCbDIRAC extension adding records to the accounting

"""

# DIRAC
from DIRAC import S_OK
from DIRAC.Core.Utilities.File import convertSizeUnits
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

  Originally, this was working for SRM only storages. It is now working with all protocols,
  but we abuse the 'SpaceToken' field to store 'SpaceReservation'

  """

  def _storeCommand(self, results):
    """ _storeCommand

    Adding records to accounting, on top of what does the derived method.

    :param dict results: something like {'ElementName': 'CERN-HIST-EOS',
                                         'Endpoint': 'httpg://srm-eoslhcb-bis.cern.ch:8443/srm/v2/server',
                                         'Free': 3264963586.10073,
                                         'Total': 8000000000.0,
                                         'SpaceReservation': 'LHCb-Disk'}
    :returns: S_OK/S_ERROR dict
    """

    res = super(FreeDiskSpaceCommand, self)._storeCommand(results)

    if not res['OK']:
      return res

    siteRes = DMSHelpers().getLocalSiteForSE(results['ElementName'])
    if not siteRes['OK']:
      return siteRes
    if not siteRes['Value']:
      return S_OK()

    spaceReservation = results.get('SpaceReservation')

    accountingDict = {
        'SpaceToken': spaceReservation,
        'Endpoint': results['Endpoint'],
        'Site': siteRes['Value']
    }

    results['Used'] = results['Total'] - results['Free']

    for sType in ['Total', 'Free', 'Used']:
      spaceTokenAccounting = SpaceToken()
      spaceTokenAccounting.setNowAsStartAndEndTime()
      spaceTokenAccounting.setValuesFromDict(accountingDict)
      spaceTokenAccounting.setValueByKey('SpaceType', sType)
      spaceTokenAccounting.setValueByKey('Space', int(convertSizeUnits(results[sType], 'MB', 'B')))

      gDataStoreClient.addRegister(spaceTokenAccounting)
    gDataStoreClient.commit()

    return S_OK()
