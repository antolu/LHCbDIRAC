from DIRAC.Core.Base.Client                     import Client
class StorageUsageClient(Client):

  def __init__(self):
    self.setServer('DataManagement/StorageUsage')
