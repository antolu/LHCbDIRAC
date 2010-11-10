########################################################################
# $Id$
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Service.FileSystem.FileManager import FileManager
from LHCbDIRAC.BookkeepingSystem.Service.FileSystem.IFileClient import IFileClient

__RCSID__ = "$Id$"

class FileSystemClient(IFileClient):
    
    #############################################################################  
    def __init__(self, fileSystemManager = FileManager(), path =""):
      super(FileSystemClient, self).__init__(fileSystemManager, path)
    