########################################################################
# $Id$
########################################################################

"""
 Base Entity System client
"""

from DIRAC                                                                   import S_ERROR
from LHCbDIRAC.BookkeepingSystem.Client.BaseESManager                        import BaseESManager


__RCSID__ = "$Id$"

#############################################################################
class BaseESClient:
  """ Basic client"""

  #############################################################################
  def __init__(self, esManager=BaseESManager(), path="/"):
    """ The Entity manager must be initialized which will be used to manipulate the databaase."""
    self.__ESManager = esManager
    result = self.getManager().getAbsolutePath(path)
    if result['OK']:
      self.__currentDirectory = result['Value']

  #############################################################################
  def list(self, path="", SelectionDict={}, SortDict={}, StartItem=0, Maxitems=0):
    """It lists the database content as a Linux File System"""
    res = self.getManager().mergePaths(self.__currentDirectory, path)
    if res['OK']:
      return self.getManager().list(res['Value'], SelectionDict, SortDict, StartItem, Maxitems)
    else:
      return S_ERROR(res['Message'])

  #############################################################################
  def getManager(self):
    """ It returns the manager whicg used to manipulate the database"""
    return self.__ESManager

  #############################################################################
  def get(self, path=""):
    """It return the actual directory"""
    return self.getManager().get(path)

  #############################################################################
  def getPathSeparator(self):
    """It returns the space separator"""
    return self.getManager().getPathSeparator()

  #############################################################################

