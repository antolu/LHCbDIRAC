########################################################################
# $Id: FileSystemClient.py,v 1.1 2008/02/29 11:54:23 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.FileSystem.FileManager import FileManager
from DIRAC.BookkeepingSystem.Agent.FileSystem.IFileClient import IFileClient

__RCSID__ = "$Id: FileSystemClient.py,v 1.1 2008/02/29 11:54:23 zmathe Exp $"

class FileSystemClient(IFileClient):
    
    #############################################################################  
    def __init__(self, fileSystemManager = FileManager(), path =""):
      super(FileSystemClient, self).__init__(fileSystemManager, path)
    