########################################################################
# $Id: BaseESClient.py,v 1.4 2008/06/09 10:31:57 zmathe Exp $
########################################################################

"""
 Base Entity System client
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.IEntitySystemClient                  import IEntitySystemClient
from DIRAC.BookkeepingSystem.Client.BaseESManager                        import BaseESManager


__RCSID__ = "$Id: BaseESClient.py,v 1.4 2008/06/09 10:31:57 zmathe Exp $"

#############################################################################
class BaseESClient(IEntitySystemClient):
  
  #############################################################################
  def __init__(self, ESManager = BaseESManager(), path ="/"):
    self.__ESManager = ESManager
    result = self.getManager().getAbsolutePath(path)
    self.__currentDirectory = result['Value']

  #############################################################################
  def list(self, path=""):
    res = self.getManager().mergePaths(self.__currentDirectory, path)
    if res['OK']:
      return self.getManager().list(res['Value'])
    else:
      return S_ERROR("list")
  
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