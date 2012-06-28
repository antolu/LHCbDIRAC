
"""
Base Entity System Manager
"""
########################################################################
# $Id$
########################################################################


from DIRAC                                                                import gLogger, S_OK, S_ERROR
import os
__RCSID__ = "$Id$"

#############################################################################
class BaseESManager:
  """Base Entity manager class"""

  #############################################################################
  def __init__(self):
    """Initialize the class members"""
    self.__fileSeparator = '/'

  #############################################################################
  def getPathSeparator(self):
    """The path separator used"""
    return self.__fileSeparator

  #############################################################################
  def list(self, path=""):
    """list the path"""
    return S_ERROR("Not Implemented!")

  #############################################################################
  def getAbsolutePath(self, path):
    """absolute path"""
    # get current working directory if empty
    if path == "" or path == None:
      path = "."
        # convert it into absolute path
    try:
      path = os.path.abspath(path)
      return S_OK(path)
    except Exception, ex:
      return S_ERROR("getAbsalutePath: "+str(ex))

  #############################################################################
  def mergePaths(self, path1, path2):
    """merge two path"""
    gLogger.debug("mergePaths(path1, path2) with input " + str(path1) + ", " + str(path2))
    path = self.getAbsolutePath(os.path.join(path1, path2))
    return path

  #############################################################################
  def get(self, path = ""):
    """the path element"""
    gLogger.warn('not implemented')
    return S_ERROR("Not implemented!")

