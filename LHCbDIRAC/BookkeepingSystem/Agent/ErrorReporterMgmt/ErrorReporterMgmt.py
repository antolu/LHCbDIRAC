########################################################################
# $Id: ErrorReporterMgmt.py,v 1.1 2008/02/29 11:53:40 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient import FileSystemClient
from DIRAC                                                     import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                   import gConfig
import os

__RCSID__ = "$Id: ErrorReporterMgmt.py,v 1.1 2008/02/29 11:53:40 zmathe Exp $"



class ErrorReporterMgmt:
  
  #############################################################################
  def __init__(self):
    self.XMLProcessingDirectory_ = gConfig.getValue("XMLProcessing", "/opt/bookkeeping/XMLProcessing/")
    self.errorDir_ = self.XMLProcessingDirectory_ + "Errors/"
    self.fileClient_ = FileSystemClient()
    
  #############################################################################
  def reportError(self, id, message, file):
    """
    
    """
    try:
      name = self.__getErrorFileName(file)
      fullName = self.errorDir_ + name +".error"
      
      f = open(fullName, 'w')
      msg = "  <Error id=\"" + str(id) + "\">" +"\n"
      msg += "    " + message +"\n"
      msg +="  </Error>";
      f.write(msg)
      f.close()
      
      self.fileClient_.rename(file, self.errorDir_ +name)
    except OSError, (errno, strerror):
      print strerror    
  
  #############################################################################  
  def __getErrorFileName(self, fileName):
    """
    
    """
    name = os.path.split(fileName)[1]
    return name
  
  #############################################################################
  
