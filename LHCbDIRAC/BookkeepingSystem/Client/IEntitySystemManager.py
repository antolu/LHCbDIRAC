########################################################################
# $Id: IEntitySystemManager.py 18173 2009-11-11 14:01:58Z zmathe $
########################################################################


"""
 Interface for entity system managers
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from LHCbDIRAC.BookkeepingSystem.Client.IEntitySystemStateless           import IEntitySystemStateless  

__RCSID__ = "$Id: IEntitySystemManager.py 18173 2009-11-11 14:01:58Z zmathe $"


#############################################################################
class IEntitySystemManager(IEntitySystemStateless):
    
  #############################################################################
  def getPathSeparator(self):
    gLogger.warn('not implemented')
  
  #############################################################################
  def getAbsolutePath(self, path):
    gLogger.warn('not implemented')

  #############################################################################
  def mergePaths(self, path1, path2):
    gLogger.warn('not implemented')
  
  #############################################################################
  def get(self, path):
    gLogger.warn('not implemented')