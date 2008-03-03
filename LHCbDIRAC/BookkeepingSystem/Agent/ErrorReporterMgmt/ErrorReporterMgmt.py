########################################################################
# $Id: ErrorReporterMgmt.py,v 1.3 2008/03/03 15:34:42 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient import FileSystemClient
from DIRAC                                                     import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                   import gConfig
import os

__RCSID__ = "$Id: ErrorReporterMgmt.py,v 1.3 2008/03/03 15:34:42 zmathe Exp $"



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
    gLogger.info("Error Report!")
    try:
      name = self.__getErrorFileName(file)
      gLogger.info("File Name:" + str(name))
      fullName = self.errorDir_ + name +".error"
      
      f = open(fullName, 'w')
      msg = "  <Error id=\"" + str(id) + "\">" +"\n"
      msg += "    " + message +"\n"
      msg +="  </Error>";
      f.write(msg)
      f.close()
      
      self.fileClient_.rename(file, self.errorDir_ +name)
      gLogger.info("Error Report End!")
    except OSError, (errno, strerror):
      gLogger.error("reportError:" + str(strerror))    
  
  #############################################################################  
  def __getErrorFileName(self, fileName):
    """
    
    """
    name = os.path.split(fileName)[1]
    return name
  
  #############################################################################
  
