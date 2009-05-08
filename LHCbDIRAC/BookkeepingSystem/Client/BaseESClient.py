########################################################################
# $Id: BaseESClient.py,v 1.6 2009/05/08 15:23:25 zmathe Exp $
########################################################################

"""
 Base Entity System client
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.IEntitySystemClient                  import IEntitySystemClient
from DIRAC.BookkeepingSystem.Client.BaseESManager                        import BaseESManager


__RCSID__ = "$Id: BaseESClient.py,v 1.6 2009/05/08 15:23:25 zmathe Exp $"

#############################################################################
class BaseESClient(IEntitySystemClient):
  
  #############################################################################
  def __init__(self, ESManager = BaseESManager(), path ="/"):
    self.__ESManager = ESManager
    result = self.getManager().getAbsolutePath(path)
    if result['OK']:
      self.__currentDirectory = result['Value']

  #############################################################################
  def list(self, path="", SelectionDict = {}, SortDict={}, StartItem=0, Maxitems=0):
    res = self.getManager().mergePaths(self.__currentDirectory, path)
    if res['OK']:
      return self.getManager().list(res['Value'], SelectionDict, SortDict, StartItem, Maxitems)
    else:
      return S_ERROR(res['Message'])
  
  #############################################################################
  def getManager(self):
    return self.__ESManager
  
  #############################################################################
  def get(self, path = ""):
    return self.getManager().get(path)
  
  #############################################################################
  def getPathSeparator(self):
    return self.getManager().getPathSeparator()
  
  #############################################################################
