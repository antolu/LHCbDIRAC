########################################################################
# $Id$
########################################################################

"""

"""

from DIRAC                                      import gLogger, S_OK, S_ERROR
import os

__RCSID__ = "$Id$"


class IFileManager(object):
  #############################################################################    
  def __init__(self):
    self.fileSeparator_ = '/'
  
  #############################################################################            
  def getPathSeparator(self):
    return self.fileSeparator_
   
  #############################################################################    
  def list(self, path=""):
    gLoogger.warn("not implemented")
    return S_ERROR()
    
  #############################################################################        
  def cp(self, frm, to):
    gLoogger.warn("not implemented")
    return S_ERROR()
      
  #############################################################################  
  def mv(self, frm, to):
    gLoogger.warn("not implemented")
    return S_ERROR()      
  
  #############################################################################  
  def mkdir(self, path):
    gLoogger.warn("not implemented")
    return S_ERROR()
 
  #############################################################################  
  def rm(self, path):
    gLoogger.warn("not implemented")
    return S_ERROR()
      
  #############################################################################  
  def rename(self, oldname, newname):
    gLoogger.warn("not implemented")
    return S_ERROR()  
  
  #############################################################################  
