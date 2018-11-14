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
    It is used for retrieving a files with meta data for a given condition.

    :param dict in_dict: It can contains the following conditions:'ConfigName',
    'ConfigVersion', 'ConditionDescription', 'EventType',
    'ProcessingPass','Production','RunNumber', 'FileType', DataQuality, StartDate, EndDate
    :return: files with meta data associated
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

    :param dict in_dict: dictionary which has the following format: in_dict = {'lfn':[],jobId:[],jobName:[]}
    :return: job meta data
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
    It is used to set the files data quality.

    :param list lfns: list of LFNs or an LFN
    :param str flag: data quality flag: OK, UNCHECKED, etc.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')

    return self._getRPC().setFileDataQuality(lfns, flag)

  #############################################################################
  def getFileAncestors(self, lfns, depth=0, replica=True):
    """
    Retrieve file ancestors.

    :param list lfns: list of LFNs
    :param int depth: depth of the processing chane
    :param bool replica: take into account the replica flag.
    :return It returns the ancestors of a file with metadata or a list of files
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')

    return self._getRPC().getFileAncestors(lfns, depth, replica)

  #############################################################################
  def getFileDescendants(self, lfns, depth=0, production=0, checkreplica=False):
    """
    Retrieve the file descendants.

    :param list lfns: list of LFNs
    :param int depth: depth of the processing chane
    :param bool replica: take into account the replica flag.
    :return: descendants of a file or a list of files.
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')

    return self._getRPC().getFileDescendants(lfns, depth, production, checkreplica)

  #############################################################################
  def addFiles(self, lfns):
    """
    It sets the replica flag Yes for a given list of files.

    :param list lfns: list of LFNs
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().addFiles(lfns)

  #############################################################################
  def removeFiles(self, lfns):
    """
    It removes the replica flag for a given list of files.

    :param list lfns: list of lfns
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().removeFiles(lfns)

  #############################################################################
  def getFileMetadata(self, lfns):
    """
    Retrieve the metadata information for a given file or a list of files.

    :param list lfns: list of LFNs
    :return: file metadata
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getFileMetadata(lfns)

  #############################################################################
  def getFileMetaDataForWeb(self, lfns):
    """
    This method only used by the web portal. It is same as getFileMetadata.

    :param list lfns: list of LFNs
    :return: file metadata
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getFileMetaDataForWeb(lfns)

  #############################################################################
  def exists(self, lfns):
    """
    It used to check the existence of a list of files in the Bookkeeping Metadata catalogue.

    :param list lfns: list of LFNs
    :return: a dictionary with the lfns {'lfn':True/False}
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().exists(lfns)

  #############################################################################
  def getRunInformation(self, in_dict):
    """
    It returns run information and statistics.

    :param dict in_dict: contains a given conditions
    :return: run statistics Number of events, luminosity, etc.
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
    For retrieve the data quality for runs or files.

    :param list runs: list of run numbers.
    :return: run or file data quality
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

    :paran list lfns: an lfn or list of lfns
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().setFilesInvisible(lfns)

  #############################################################################
  def setFilesVisible(self, lfns):
    """
    It is used to set the file(s) invisible in the database

    :param list lfns: an lfn or list of lfns
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().setFilesVisible(lfns)

  #############################################################################
  def getFilesWithGivenDataSets(self, in_dict):
    """
    For retrieving list of files.

    :param dict in_dict: contains a given conditions
    :return: list of files
    """
    return self.getFiles(in_dict)

  #############################################################################
  def getFileTypeVersion(self, lfns):
    """
    For retrieving the file type version

    :param list lfns: list of lfns
    :return: file type version
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getFileTypeVersion(lfns)

  #############################################################################
  def getDirectoryMetadata(self, lfns):
    """
    For retrieving  meta data information for a given directory.

    :param list lfns: list of lfns
    :return: directory metadata
    """
    if isinstance(lfns, basestring):
      lfns = lfns.split(';')
    return self._getRPC().getDirectoryMetadata(lfns)

  #############################################################################
  def getRunsForFill(self, fillid):
    """
    For retrieving a list of runs.

    :param long fillid: fill number
    :return: runs for a given fill
    """
    try:
      fill = long(fillid)
    except ValueError, ex:
      return S_ERROR(ex)
    return self._getRPC().getRunsForFill(fill)

  #############################################################################
  def deleteSimulationConditions(self, simid):
    """It deletes a given simulation condition

    :param long simid: simulation condition identifier
    """
    try:
      simid = long(simid)
    except ValueError, ex:
      return S_ERROR(ex)
    return self._getRPC().deleteSimulationConditions(simid)

  #############################################################################
  def getJobInputOutputFiles(self, diracjobids):
    """
    For retrieving input/output for a given Dirac job

    :param list diracjobids: list of dirac job ids.
    :return: input/output file(s) for a given dirac job
    """
    if isinstance(diracjobids, (int, long)):
      diracjobids = [diracjobids]
    return self._getRPC().getJobInputOutputFiles(diracjobids)

  def fixRunLuminosity(self, runnumbers):
    """
    For fixing the luminosity for a given run(s)

    :param list runnumbers: list of run numbers.
    """
    if isinstance(runnumbers, (int, long)):
      runnumbers = [runnumbers]
    return self._getRPC().fixRunLuminosity(runnumbers)

  # The following method names are changed in the Bookkeeping client.

  #############################################################################
  def getFiles(self, in_dict):
    """
    It returns a list of files for a given condition.

    :param dict in_dict: contains a given conditions
    :return: list of files
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
    """
    For retrieving the run status.

    :param list runs: list of runs
    :return: run status (finished, or not finished)
    """
    runnumbers = []
    if isinstance(runs, basestring):
      runnumbers = [int(run) for run in runs.split(';')]
    elif isinstance(runs, (int, long)):
      runnumbers += [runs]
    else:
      runnumbers = runs
    return self._getRPC().getRunStatus(runnumbers)

  def getProcessingPass(self, in_dict, path=None):
    """
      This method is used to recursively browse the processing pass.

      :param dict in_dict: contains a given conditions: ConfigName', 'ConfigVersion', 'ConditionDescription',
      'Production','RunNumber', 'EventType'
      :param str path: To start the browsing you have to define the path as a root: path = '/'
      :return: processing pass for a given conditions. Note: it returns a list with two dictionary. First dictionary
      contains the processing passes
      while the second dictionary contains the event types.
      """
    if path is None:
      path = '/'
    return self._getRPC().getProcessingPass(in_dict, path)

  def getProductionFilesStatus(self, productionid=None, lfns=None):
    """
    Status of the files, which belong to a given production.

    :param int productionid:
    :return: the file status in the bkk for a given production or a list of lfns.
    """
    if lfns is None:
      lfns = []
    return self._getRPC().getProductionFilesStatus(productionid, lfns)


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
