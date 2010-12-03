########################################################################
# $Id: FileSystemClient.py 18172 2009-11-11 14:01:45Z zmathe $
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Agent.FileSystem.FileManager import FileManager
from LHCbDIRAC.BookkeepingSystem.Agent.FileSystem.IFileClient import IFileClient

__RCSID__ = "$Id: FileSystemClient.py 18172 2009-11-11 14:01:45Z zmathe $"

class FileSystemClient(IFileClient):
    
    #############################################################################  
    def __init__(self, fileSystemManager = FileManager(), path =""):
      super(FileSystemClient, self).__init__(fileSystemManager, path)
    