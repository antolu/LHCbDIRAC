# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Client/FileReport.py,v 1.2 2008/06/03 14:26:26 atsareg Exp $

"""
  FileReport class encapsulates methods to report file status in the
  production environment in failover safe way
"""

__RCSID__ = "$Id: FileReport.py,v 1.2 2008/06/03 14:26:26 atsareg Exp $"

import datetime
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest


class FileReport:

  def __init__(self):
    self.productionSvc = RPCClient('ProductionManagement/ProductionManager')
    self.statusDict = {}
    self.production = None

  def setFileStatus(self,production,lfn,status,sendFlag=False):
    """ Set file status in the contesxt of the given transformation
    """

    if not self.production:
      self.production = production

    result = S_OK()
    if sendFlag:
      sendList = []
      if self.statusDict:
        for lfn_s,status_s in self.statusDict.items():
          sendList.append((lfn_s,status_s))
      sendList.append((lfn,status))
      result = self.productionSvc.setFileStatusForTransformation(production,sendList)
      if result['OK']:
        return result

    # Add the file status info to the internal cache for later retry
    self.statusDict[lfn] = status
    return result

  def setCommonStatus(self,status):
    """ Set common status for all files in the internal cache
    """

    for lfn in self.statusDict.keys():
      self.statusDict[lfn] = status

    return S_OK()

  def commit(self):
    """ Commit pending file status update records
    """

    sendList = []
    if self.statusDict:
      for lfn_s,status_s in self.statusDict.items():
          sendList.append((lfn_s,status_s))
      result = self.productionSvc.setFileStatusForTransformation(production,sendList)
    else:
      return S_OK()

    if result['OK']:
      self.statusDict = {}

    return result

  def generateRequest(self):
    """ Commit the accumulated records and generate request eventually
    """

    result = self.commit()

    request = None
    if not result['OK']:
      # Generate Request
      request = RequestContainer()
      request.addSubRequest(DISETSubRequest(result['rpcStub']).getDictionary(),'filestatus_procdb')

    return S_OK(request)