"""
in_dict = {'EventTypeId': 93000000,
        'ConfigVersion': 'Collision10',
        'ProcessingPass': '/Real Data',
        'ConfigName': 'LHCb',
        'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
        'Production':7421
         }
"""

import tempfile

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.DISET.TransferClient import TransferClient
from LHCbDIRAC.BookkeepingSystem.Client import JEncoder

__RCSID__ = "$Id$"


class BookkeepingClient(Client):
  """ This class expose the methods of the Bookkeeping Service"""

  def __init__(self, url=None, **kwargs):
    """
    c'tor
    :param str url: can specify a specific URL
    """
    Client.__init__(self, **kwargs)
    self.setServer('Bookkeeping/BookkeepingManager')
    if url:
      self.setServer(url)
    self.setTimeout(3600)

  #############################################################################
  def getAvailableFileTypes(self):
    """
    It returns all the available files which are registered to the bkk.
    """
    retVal = self._getRPC().getAvailableFileTypes()
    if retVal['OK']:
      records = []
      parameters = ["FileType", "Description"]
      for record in retVal['Value']:
        records += [list(record)]
      return S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      return retVal

  #############################################################################
  @staticmethod
  def getFilesWithMetadata(in_dict):
    """
    It returns the files for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName',
    'ConfigVersion', 'ConditionDescription', 'EventType',
    'ProcessingPass','Production','RunNumber', 'FileType', DataQuality, StartDate, EndDate
    """
    in_dict = dict(in_dict)
    bkk = TransferClient('Bookkeeping/BookkeepingManager')
    params = JEncoder.dumps(in_dict)
    file_name = tempfile.NamedTemporaryFile()
    retVal = bkk.receiveFile(file_name.name, params)
    if not retVal['OK']:
      return retVal
    else:
      value = JEncoder.load(open(file_name.name))
      file_name.close()
      return S_OK(value)

  #############################################################################
  def bulkJobInfo(self, in_dict):
    """
    It returns the job metadata information for a given condition:
    -a list of lfns
    - a list of DIRAC job ids
    - a list of jobNames
    in_dict = {'lfn':[],jobId:[],jobName:[]}

    """
    conditions = {}
    if isinstance(in_dict, basestring):
      conditions['lfn'] = in_dict.split(';')
    elif isinstance(in_dict, list):
      conditions['lfn'] = in_dict
    else:
      conditions = in_dict

    return self._getRPC().bulkJobInfo(conditions)

  #############################################################################
  def setFileDataQuality(self, lfns, flag):
    """
    It is used to set the files data quality flags. The input parameters is an
    lfn or a list of lfns and the data quality flag.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')

    return self._getRPC().setFileDataQuality(lfns, flag)

  #############################################################################
  def getFileAncestors(self, lfns, depth=0, replica=True):
    """
    It returns the ancestors of a file or a list of files. It also returns the metadata of the ancestor files.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')

    return self._getRPC().getFileAncestors(lfns, depth, replica)

  #############################################################################
  def getFileDescendants(self, lfns, depth=0, production=0, checkreplica=False):
    """
    It returns the descendants of a file or a list of files.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')

    return self._getRPC().getFileDescendants(lfns, depth, production, checkreplica)

  #############################################################################
  def addFiles(self, lfns):
    """
    It sets the replica flag Yes for a given list of files.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().addFiles(lfns)

  #############################################################################
  def removeFiles(self, lfns):
    """
    It removes the replica flag for a given list of files.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().removeFiles(lfns)

  #############################################################################
  def getFileMetadata(self, lfns):
    """
    It returns the metadata information for a given file or a list of files.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getFileMetadata(lfns)

  #############################################################################
  def getFileMetaDataForWeb(self, lfns):
    """
    This method only used by the web portal. It is same as getFileMetadata.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getFileMetaDataForWeb(lfns)

  #############################################################################
  def exists(self, lfns):
    """
    It used to check the existence of a list of files in the Bookkeeping Metadata catalogue.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().exists(lfns)

  #############################################################################
  def getRunInformation(self, in_dict):
    """
    It returns run information and statistics.
    """
    if 'Fields' not in in_dict:
      in_dict['Fields'] = ['ConfigName', 'ConfigVersion', 'JobStart', 'JobEnd', 'TCK',
                           'FillNumber', 'ProcessingPass', 'ConditionDescription', 'CONDDB', 'DDDB']
    if 'Statistics' in in_dict and len(in_dict['Statistics']) == 0:
      in_dict['Statistics'] = ['NbOfFiles', 'EventStat', 'FileSize', 'FullStat',
                               'Luminosity', 'InstLumonosity', 'EventType']

    return self._getRPC().getRunInformation(in_dict)

  #############################################################################
  def getRunFilesDataQuality(self, runs):
    """
    It returns the data quality of files for set of runs.
    Input parameters:
    runs: list of run numbers.
    """
    if isinstance(runs, basestring):
      runs = runs.split(';')
    elif isinstance(runs, (int, long)):
      runs = [runs]
    return self._getRPC().getRunFilesDataQuality(runs)

  #############################################################################
  def setFilesInvisible(self, lfns):
    """
    It is used to set the file(s) invisible in the database
    Input parameter:
    lfns: an lfn or list of lfns
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().setFilesInvisible(lfns)

  #############################################################################
  def setFilesVisible(self, lfns):
    """
    It is used to set the file(s) invisible in the database
    Input parameter:
    lfns: an lfn or list of lfns
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().setFilesVisible(lfns)

  #############################################################################
  def getFilesWithGivenDataSets(self, values):
    """
    It returns a list of files for a given condition.
    """
    return self.getFiles(values)

  #############################################################################
  def getFileTypeVersion(self, lfns):
    """
    It returns the file type version of given lfns
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getFileTypeVersion(lfns)

  #############################################################################
  def getDirectoryMetadata(self, lfns):
    """
    It returns metadata informatiom for a given directory.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getDirectoryMetadata(lfns)

  #############################################################################
  def getRunsForFill(self, fillid):
    """
    It returns a list of runs for a given FILL
    """
    try:
      fill = long(fillid)
    except ValueError, ex:
      return S_ERROR(ex)
    return self._getRPC().getRunsForFill(fill)

  #############################################################################
  def deleteSimulationConditions(self, simid):
    """It deletes a given simulation condition
    """
    try:
      simid = long(simid)
    except ValueError, ex:
      return S_ERROR(ex)
    return self._getRPC().deleteSimulationConditions(simid)

  #############################################################################
  def getJobInputOutputFiles(self, diracjobids):
    """It returns the input and output files for a given DIRAC jobid"""
    if isinstance(diracjobids, (int, long)):
      diracjobids = [diracjobids]
    return self._getRPC().getJobInputOutputFiles(diracjobids)

  def fixRunLuminosity(self, runnumbers):
    """
    we can fix the luminosity of the runs/
    """
    if isinstance(runnumbers, (int, long)):
      runnumbers = [runnumbers]
    return self._getRPC().fixRunLuminosity(runnumbers)

  # The following method names are changed in the Bookkeeping client.

  #############################################################################
  def getFiles(self, in_dict):
    """
    It returns a list of files for a given condition.
    """
    in_dict = dict(in_dict)
    bkk = TransferClient('Bookkeeping/BookkeepingManager')
    in_dict['MethodName'] = 'getFiles'
    params = JEncoder.dumps(in_dict)
    file_name = tempfile.NamedTemporaryFile()
    retVal = bkk.receiveFile(file_name.name, params)
    if not retVal['OK']:
      return retVal
    else:
      value = JEncoder.load(open(file_name.name))
      file_name.close()
      return value

  def getRunStatus(self, runs):
    "it return the status of the runs"
    runnumbers = []
    if isinstance(runs, basestring):
      runnumbers = [int(run) for run in runs.split(';')]
    elif isinstance(runs, (int, long)):
      runnumbers += [runs]
    else:
      runnumbers = runs
    return self._getRPC().getRunStatus(runnumbers)


class BKClientWithRetry():
  """
  Utility class wrapping BKClient with retries
  """

  def __init__(self, bkClient=None, retries=None):
    if not bkClient:
      bkClient = BookkeepingClient()
    self.bk = bkClient
    self.retries = retries if retries else 5
    self.method = None

  def __getattr__(self, x):
    self.method = x
    return self.__executeMethod

  def __executeMethod(self, *args, **kwargs):
    fcn = getattr(self.bk, self.method)
    for _i in xrange(self.retries):
      res = fcn(*args, **kwargs)
      if res['OK']:
        break
    return res
