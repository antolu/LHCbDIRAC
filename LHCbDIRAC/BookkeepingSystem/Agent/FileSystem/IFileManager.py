########################################################################
# $Id: IFileManager.py 21961 2010-02-21 22:27:58Z rgracian $
########################################################################

"""

"""

from DIRAC                                      import gLogger, S_OK, S_ERROR
import os

__RCSID__ = "$Id: IFileManager.py 21961 2010-02-21 22:27:58Z rgracian $"


class IFileManager(object):
  #############################################################################    
  def __init__(self):
    self.fileSeparator_ = '/'
  
  #############################################################################            
  def getPathSeparator(self):
    return self.fileSeparator_
   
  #############################################################################    
  def list(self, path=""):
    gLogger.warn("not implemented")
    return S_ERROR()
    
  #############################################################################        
  def cp(self, frm, to):
    gLogger.warn("not implemented")
    return S_ERROR()
      
  #############################################################################  
  def mv(self, frm, to):
    gLogger.warn("not implemented")
    return S_ERROR()      
  
  #############################################################################  
  def mkdir(self, path):
    gLogger.warn("not implemented")
    return S_ERROR()
 
  #############################################################################  
  def rm(self, path):
    gLogger.warn("not implemented")
    return S_ERROR()
      
  #############################################################################  
  def rename(self, oldname, newname):
    gLogger.warn("not implemented")
    return S_ERROR()  
  
  #############################################################################  
