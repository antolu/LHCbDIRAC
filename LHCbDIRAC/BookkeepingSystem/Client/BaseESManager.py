########################################################################
# $Id: BaseESManager.py,v 1.2 2008/06/09 10:16:55 zmathe Exp $
########################################################################


"""
Base Entity System Manager
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.IEntitySystemManager                 import IEntitySystemManager 

__RCSID__ = "$Id: BaseESManager.py,v 1.2 2008/06/09 10:16:55 zmathe Exp $"

############################################################################# 
class BaseESManager(IEntitySystemManager):
      
  #############################################################################
  def __init__(self):
    self.__fileSeparator = '/'
  
  #############################################################################
  def getPathSeparator(self):
    return self.__fileSeparator
  
  #############################################################################
  def list(self, path=""):
    return S_ERROR("Not Implemented!")
  
  #############################################################################
  def getAbsolutePath(self, path):
    # get current working directory if empty
    if path == "" or path == None:
      path = "."        
        # convert it into absolute path
    try:    
      path = os.path.abspath(path)
      return S_OK(path)
    except:
      return S_ERROR("getAbsalutePath")
  
  #############################################################################
  def mergePaths(self, path1, path2):
    gLogger.debug("mergePaths(path1, path2) with input " + str(path1) + ", " + str(path2))        
    return self.getAbsolutePath(os.path.join(path1, path2)) 

  #############################################################################
  def get(self, path = ""):
    gLogger.warn('not implemented')
    return S_ERROR("Not implemented!")

