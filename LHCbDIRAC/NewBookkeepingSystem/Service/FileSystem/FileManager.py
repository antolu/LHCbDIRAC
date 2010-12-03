########################################################################
# $Id$
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Service.FileSystem.IFileManager import IFileManager
from LHCbDIRAC.BookkeepingSystem.Service.FileSystem.IFileClient  import IFileClient
from DIRAC                                                       import gLogger, S_OK, S_ERROR

import os
import types
import shutil

__RCSID__ = "$Id$"

class FileManager(IFileManager):
  
  #############################################################################
  def __init__(self):
    super(FileManager, self).__init__()
    self._IFileManager__fileSeparator_ = os.sep    
      
  #############################################################################
  def list(self, path=""):    
    """
    
    """
    fileList = os.listdir(path)
    files = list()
    for file in fileList:        
      fullPath = path + self.getPathSeparator() + file
      files.append(fullPath)
    return files
  
  #############################################################################  
  def cp(self, frm, to):
   """
   
   """
   try:
     frompath = frm
     topath = to + self.getPathSeparator()  + os.path.split(frompath)[1]
                 
     shutil.copytree(frompath, topath)
     return S_OK()
   except OSError, (errno, strerror):
     gLogger.error(strerror)
     return S_ERROR(strerror)
        
  #############################################################################  
  def mv(self, frm, to):
    """
    
    """
    n = len(frm)
    try:
      if n > 0:
        for i in range(n):
          frompath = frm[i]
          if os.path.isdir(frompath):
            topath = to + self.getPathSeparator()  + os.path.split(frompath)[1]
            self.mkdir(topath)
          else:
            topath = to
          shutil.move(frompath, topath)
          return S_OK()
    except OSError, (errno, strerror):
      gLogger.error(strerror)
      return S_ERROR(strerror)
  
  #############################################################################          
  def rename(self, oldname, newname):
    """
    
    """
    try:
      os.rename(oldname, newname)
      return S_OK()
    except OSError, (errno, strerror):
      gLogger.error(strerror)
      return S_ERROR(strerror)
  
  #############################################################################          
  def rm(self, name):
    """
    
    """
    try:
      os.remove(name)
      return S_OK()
    except OSError, (errno, strerror):
      gLogger.error(strerror)
      return S_ERROR(strerror)
      
  #############################################################################          
  def mkdir(self, path):
    """
    
    """
    try:
      os.mkdir(path)
      return S_OK()
    except OSError, (errno, strerror):
      gLogger.error(strerror)
      return S_ERROR(strerror)
  
  #############################################################################  
  