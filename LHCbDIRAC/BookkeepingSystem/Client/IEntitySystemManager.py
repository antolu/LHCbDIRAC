########################################################################
# $Id$
########################################################################


"""
 Interface for entity system managers
"""

from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.IEntitySystemStateless           import IEntitySystemStateless  

__RCSID__ = "$Id$"


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