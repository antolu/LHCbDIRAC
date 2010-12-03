########################################################################
# $Id: IFileClient.py 18172 2009-11-11 14:01:45Z zmathe $
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Agent.FileSystem.IFileManager import IFileManager

__RCSID__ = "$Id: IFileClient.py 18172 2009-11-11 14:01:45Z zmathe $"

class IFileClient(object):
  
  #############################################################################    
  def __init__(self, FileManager = IFileManager(), path =""):
    self.fielManager_ = FileManager

  #############################################################################  
  def list(self, path=""):
    return self.getManager().list(path)

  #############################################################################  
  def cp(self, frm, to):
    return self.getManager().cp(frm, to)
  
  #############################################################################    
  def mv(self, frm, to):
    return self.getManager().mv(frm, to)
  
  #############################################################################    
  def mkdir(self, path):
    return self.getManager().mkdir(path)
  
  #############################################################################  
  def rm(self, path):
    return self.getManager().rm(path)
    
  #############################################################################  
  def getManager(self):
    return self.fielManager_
    
  #############################################################################  
  def rename(self, oldname, newname):
    return self.getManager().rename(oldname, newname)
  
  #############################################################################    
  def getPathSeparator(self):
    return self.getManager().getPathSeparator()
  
  