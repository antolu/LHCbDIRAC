########################################################################
# $Id: IEntitySystemManager.py,v 1.2 2008/06/09 10:16:55 zmathe Exp $
########################################################################


"""
 Interface for entity system managers
"""

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IEntitySystemManager.py,v 1.2 2008/06/09 10:16:55 zmathe Exp $"

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