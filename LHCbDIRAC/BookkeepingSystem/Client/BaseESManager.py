
"""
Base Entity System Manager
"""
########################################################################
# $Id: BaseESManager.py 69359 2013-08-08 13:57:13Z phicharp $
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
  def list(self, path="/", selectionDict=None, sortDict=None, startItem=0, maxitems=0):
    """list the path"""
    selectionDict = selectionDict if selectionDict is not None else {}
    sortDict = sortDict if sortDict is not None else {}
    gLogger.error('This method is not implemented!'+ (str(self.__class__)))
    gLogger.error(str(path))
    gLogger.error(str(selectionDict))
    gLogger.error(str(sortDict))
    gLogger.error(str(startItem))
    gLogger.error(str(maxitems))
    return S_ERROR("Not Implemented!")

  #############################################################################
  @staticmethod
  def getAbsolutePath(path):
    """absolute path"""
    # get current working directory if empty
    if path == "" or path == None:
      path = "."
        # convert it into absolute path
    try:
      path = os.path.abspath(path)
      return S_OK(path)
    except IOError, ex:
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
    gLogger.warn('not implemented'+path+str(self.__class__))
    return S_ERROR("Not implemented!")

