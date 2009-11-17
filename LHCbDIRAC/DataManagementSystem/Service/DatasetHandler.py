""" DISET request handler base class for the DatasetDB.
"""
from DIRAC                                                  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                        import RequestHandler
from DIRAC.DataManagementSystem.DB.DatasetDB                import DatasetDB
from DIRAC.DataManagementSystem.Client.Dataset              import Dataset
from DIRAC.Resources.Catalog.FileCatalog                    import FileCatalog
from types                                                  import *

# This is a global instance of the DataLoggingDB class
datasetDB = False

def initializeDatasetHandler(serviceInfo):
  global datasetDB
  datasetDB = DatasetDB()
  return S_OK()

class DatasetHandler(RequestHandler):

  types_publishDataset = [StringType,StringType,StringType,ListType,DictType]
  def export_publishDataset(self,handle,description,longDescription,lfns,metadataTags):
    """ This method will create the data set in the LFC the datasetDB
    """
    authorDN = self._clientTransport.peerCredentials['DN']
    authorGroup = self._clientTransport.peerCredentials['group']
    authorName = self._clientTransport.peerCredentials['username']
    type = 'user'
    if authorGroup == 'lhcb_prod':
      type = 'production'
   
    res = datasetDB.datasetExists(handle)
    if not res['OK']:
      return res
    elif res['Value']:
      return S_ERROR("DatasetHandler.publishDataset: A dataset with this handle already exists.")

    oDataset = Dataset()
    oDataset.setHandle(handle)
    oDataset.setLfns(lfns)
    # First create the links in the LFC
    res = oDataset.createDataset()
    if not res['OK']:
      result = oDataset.removeDataset()
      return res
    # Publish the dataset in the Datasets table
    res = datasetDB.publishDataset(handle,description,longDescription,authorDN,authorGroup,type)
    if not res['OK']:
      oDataset.removeDataset()
      return res
    # Add the metadata for the dataset to the DatasetParameters table
    res = datasetDB.addDatasetParameters(handle,metadataTags)
    if not res['OK']:
      oDataset.removeDataset()
      datasetDB.removeDataset(handle)
      return res
    # Add the logging message
    message = 'Created'
    datasetDB.updateDatasetLogging(handle,message,authorDN)
    return res

  types_deleteDataset = [StringType]
  def export_deleteDataset(self,handle):
    """ This method will remove the supplied dataset from the catalog and the LFC
    """
    res = datasetDB.removeDataset(handle)
    if res['OK']:
      oDataset = Dataset()
      oDataset.setHandle(handle)
      res = oDataset.removeDataset()
    return res

  types_removeFileFromDataset = [StringType,StringType]
  def export_removeFileFromDataset(self,handle,lfn):
    """ This method will remove the supplied file from the dataset
    """
    oDataset = Dataset()
    oDataset.setHandle(handle)
    res = oDataset.removeFile(lfn)
    if res['OK']:
      authorDN = self._clientTransport.peerCredentials['DN']
      message = 'Removed %s' % lfn
      res = datasetDB.updateDatasetLogging(handle,message,authorDN)
    return res

  types_getAllDatasets = []
  def export_getAllDatasets(self):
    """ Get all the dataset handles
    """
    return datasetDB.getAllDatasets()

  types_getDatasetHistory = [StringType]
  def export_getDatasetHistory(self,handle):
    """ Get the history for a single dataset
    """
    return datasetDB.getDatasetHistory(handle)
