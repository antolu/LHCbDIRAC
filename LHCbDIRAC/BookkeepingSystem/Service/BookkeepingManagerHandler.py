""" BookkeepingManager service is the front-end to the Bookkeeping database
"""

import cPickle

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
from DIRAC.ConfigurationSystem.Client.Config import gConfig

from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient import BookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.XMLFilesReaderManager import XMLFilesReaderManager
from LHCbDIRAC.BookkeepingSystem.Client import JEncoder
from LHCbDIRAC.BookkeepingSystem.DB.Utilities import checkEnoughBKArguments

__RCSID__ = "$Id$"

# pylint: disable=invalid-name


dataMGMT_ = None

reader_ = None

global default
default = 'ALL'

__eventTypeCache = None


def initializeBookkeepingManagerHandler(serviceInfo):
  """ Put here necessary initializations needed at the service start
  """
  global dataMGMT_
  dataMGMT_ = BookkeepingDatabaseClient()

  global reader_
  reader_ = XMLFilesReaderManager()

  global __eventTypeCache
  __eventTypeCache = {}

  return S_OK()


class BookkeepingManagerHandler(RequestHandler):

  """
  Bookkeeping Service class. It serves the requests made the users by using the BookkeepingClient.
  """

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """ Initializes the variables used to identify queries, which are not containing enough conditions.
    """

    bkkSection = getServiceSection("Bookkeeping/BookkeepingManager")
    if not bkkSection:
      cls.email = 'lhcb-bookkeeping@cern.ch'
      cls.forceExecution = False
    else:
      cls.email = gConfig.getValue(cfgPath(bkkSection, 'Email'), 'lhcb-bookkeeping@cern.ch')
      cls.forceExecution = gConfig.getValue(cfgPath(bkkSection, 'ForceExecution'), False)
    gLogger.info("Email used to track queries: %s forceExecution" % cls.email, cls.forceExecution)
    return S_OK()
  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [basestring]

  @staticmethod
  def export_echo(inputstring):
    """ Echo input to output
    """
    return S_OK(inputstring)

  #############################################################################
  types_sendBookkeeping = [basestring, basestring]

  def export_sendBookkeeping(self, name, xml):
    """
    more info in the BookkeepingClient.py
    """
    return self.export_sendXMLBookkeepingReport(xml)

  #############################################################################
  types_sendXMLBookkeepingReport = [basestring]

  @staticmethod
  def export_sendXMLBookkeepingReport(xml):
    """
    This method is used to upload an xml report which is produced after when the job successfully finished.
    The input parameter 'xml' is a string which contains various information (metadata)
    about the finished job in the Grid in an XML format.
    :param str xml: bookkeeping report
    """
    result = S_ERROR()
    try:
      retVal = reader_.readXMLfromString(xml)
      if not retVal['OK']:
        result = S_ERROR(retVal['Message'])
      elif retVal['Value'] == '':
        result = S_OK("The send bookkeeping finished successfully!")
      else:
        result = retVal
    except Exception as x:
      errorMsg = "XML processing error"
      gLogger.exception(errorMsg, lException=x)
      result = S_ERROR(errorMsg)
    return result

  #############################################################################
  types_getAvailableSteps = [dict]

  @staticmethod
  def export_getAvailableSteps(in_dict):
    """
    It returns all the available steps which corresponds to a given conditions.
    The in_dict contains the following conditions: StartDate, StepId, InputFileTypes, OutputFileTypes,
        ApplicationName, ApplicationVersion, OptionFiles, DDDB, CONDDB, ExtraPackages, Visible,
    ProcessingPass, Usable, RuntimeProjects, DQTag, OptionsFormat, StartItem, MaxItem
    """
    return dataMGMT_.getAvailableSteps(in_dict)

  #############################################################################
  types_getRuntimeProjects = [dict]

  @staticmethod
  def export_getRuntimeProjects(in_dict):
    """
    It returns a runtime project for a given step. The input parameter is a in_dictionary which has only one key StepId
    """
    return dataMGMT_.getRuntimeProjects(in_dict)

  #############################################################################
  types_getStepInputFiles = [int]

  @staticmethod
  def export_getStepInputFiles(stepId):
    """
    It returns the input files for a given step.
    """
    result = S_ERROR()
    retVal = dataMGMT_.getStepInputFiles(stepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType', 'Visible']
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_setStepInputFiles = [int, list]

  @staticmethod
  def export_setStepInputFiles(stepid, files):
    """
     It is used to set input file types to a Step.
    """
    return dataMGMT_.setStepInputFiles(stepid, files)

  #############################################################################
  types_setStepOutputFiles = [int, list]

  @staticmethod
  def export_setStepOutputFiles(stepid, files):
    """
    It is used to set output file types to a Step.
    """
    return dataMGMT_.setStepOutputFiles(stepid, files)

  #############################################################################
  types_getStepOutputFiles = [int]

  @staticmethod
  def export_getStepOutputFiles(stepId):
    """
    It returns the output file types for a given Step.
    """
    result = S_ERROR()
    retVal = dataMGMT_.getStepOutputFiles(stepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType', 'Visible']
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableFileTypes = []

  @staticmethod
  def export_getAvailableFileTypes():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableFileTypes()

  #############################################################################
  types_insertFileTypes = [basestring, basestring, basestring]

  @staticmethod
  def export_insertFileTypes(ftype, desc, fileType):
    """
    It is used to register a file type. It has the following input parameters:
    :param str ftype: file type; for example: COOL.DST
    :param str desc: a short description which describes the file content
    :paran str fileType: the file format such as ROOT, POOL_ROOT, etc.
    """
    return dataMGMT_.insertFileTypes(ftype, desc, fileType)

  #############################################################################
  types_insertStep = [dict]

  @staticmethod
  def export_insertStep(in_dict):
    """
    It used to insert a step to the Bookkeeping Metadata Catalogue.
    The imput parameter is a dictionary which contains the steps attributes.
    For example: Dictionary format:
    {'Step': {'ApplicationName': 'DaVinci',
    'Usable': 'Yes',
    'StepId': '',
    'ApplicationVersion': 'v29r1', 'ext-comp-1273':
    'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '', 'StepName': 'davinci prb2',
    'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)', 'Visible': 'Y', 'DDDB': '',
    'OptionFiles': '', 'CONDDB': ''}, 'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}],
    'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}],'RuntimeProjects':[{StepId:13878}]}
    """
    return dataMGMT_.insertStep(in_dict)

  #############################################################################
  types_deleteStep = [int]

  @staticmethod
  def export_deleteStep(stepid):
    """
    It used to delete a given step.
    """
    return dataMGMT_.deleteStep(stepid)

  #############################################################################
  types_updateStep = [dict]

  @staticmethod
  def export_updateStep(in_dict):
    """
    It is used to modify the step attributes.
    """
    return dataMGMT_.updateStep(in_dict)

  ##############################################################################
  types_getAvailableConfigNames = []

  @staticmethod
  def export_getAvailableConfigNames():
    """
    It returns all the available configuration names which are used.
    """
    retVal = dataMGMT_.getAvailableConfigNames()
    if retVal['OK']:
      records = []
      parameters = ['Configuration Name']
      for record in retVal['Value']:
        records += [list(record)]
      return S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      return retVal

  #############################################################################
  types_getConfigVersions = [dict]

  @staticmethod
  def export_getConfigVersions(in_dict):
    """
    It returns all the available configuration version for a given condition.
    Input parameter is a dictionary which has the following key: 'ConfigName'
    For example: in_dict = {'ConfigName':'MC'}
    """
    result = S_ERROR()
    configName = in_dict.get('ConfigName', default)
    retVal = dataMGMT_.getConfigVersions(configName)
    if retVal['OK']:
      records = []
      parameters = ['Configuration Version']
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getConditions = [dict]

  @staticmethod
  def export_getConditions(in_dict):
    """
    It returns all the available conditions for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'EventType'
    For example: in_dict = {'ConfigName':'MC','ConfigVersion':'MC10'}
    """
    result = S_ERROR()
    ok = True
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))

    if 'EventTypeId' in in_dict:
      gLogger.verbose('EventTypeId will be not accepted! Please change it to EventType')

    retVal = dataMGMT_.getConditions(configName, configVersion, evt)
    if retVal['OK']:
      values = retVal['Value']
      sim_parameters = ['SimId',
                        'Description',
                        'BeamCondition',
                        'BeamEnergy',
                        'Generator',
                        'MagneticField',
                        'DetectorCondition',
                        'Luminosity',
                        'G4settings']
      daq_parameters = ['DaqperiodId', 'Description',
                        'BeamCondition', 'BeanEnergy',
                        'MagneticField', 'VELO',
                        'IT', 'TT', 'OT', 'RICH1',
                        'RICH2', 'SPD_PRS', 'ECAL',
                        'HCAL', 'MUON', 'L0', 'HLT',
                        'VeloPosition']
      sim_records = []
      daq_records = []

      if len(values) > 0:
        for record in values:
          if record[0] != None:
            sim_records += [[record[0], record[2],
                             record[3], record[4],
                             record[5], record[6],
                             record[7], record[8],
                             record[9]]]
          elif record[1] != None:
            daq_records += [[record[1], record[10], record[11],
                             record[12], record[13], record[14],
                             record[15], record[16], record[17],
                             record[18], record[19], record[20],
                             record[21], record[22], record[23],
                             record[24], record[25], record[26]]]
          else:
            result = S_ERROR("Condition does not existis!")
            ok = False
      if ok:
        result = S_OK([{'ParameterNames': sim_parameters,
                        'Records': sim_records,
                        'TotalRecords': len(sim_records)}, {'ParameterNames': daq_parameters,
                                                            'Records': daq_records,
                                                            'TotalRecords': len(daq_records)}
                       ])
    else:
      result = retVal

    return result

  #############################################################################
  types_getProcessingPass = [dict, basestring]

  @staticmethod
  def export_getProcessingPass(in_dict, path=None):
    """
    It returns the processing pass for a given conditions.
    Input parameter is a dictionary and a path (string) which has the following keys:
    'ConfigName', 'ConfigVersion', 'ConditionDescription','Production', 'RunNumber', 'EventType'
    This method is used to recursively browse the processing pass.
    To start the browsing you have to define the path as a root: path = '/'
    Note: it returns a list with two dictionary. First dictionary contains the processing passes
    while the second dictionary contains the event types.
    """
    if path is None:
      path = '/'
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    prod = in_dict.get('Production', default)
    runnb = in_dict.get('RunNumber', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    return dataMGMT_.getProcessingPass(configName, configVersion, conddescription, runnb, prod, evt, path)

  ############################################################################
  types_getStandardProcessingPass = [dict, basestring]

  def export_getStandardProcessingPass(self, in_dict, path):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = self.export_getProcessingPass(in_dict, path)
    if retVal['OK']:
      result = S_OK(retVal['Value'])
    else:
      result = retVal
    return result

  #############################################################################
  types_getProductions = [dict]

  @staticmethod
  def export_getProductions(in_dict):
    """
    It returns the productions for a given conditions.
    Input parameter is a dictionary which has the following keys:
    'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass'
    """
    result = S_ERROR()
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    processing = in_dict.get('ProcessingPass', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    replicaFlag = in_dict.get('ReplicaFlag', 'Yes')
    ftype = in_dict.get('FileType', default)
    visible = in_dict.get('Visible', 'Y')
    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    retVal = dataMGMT_.getProductions(
        configName, configVersion, conddescription, processing, evt, visible, ftype, replicaFlag)
    if retVal['OK']:
      records = []
      parameters = ['Production/RunNumber']
      for record in retVal['Value']:
        records += [[record[0]]]
      result = S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getFileTypes = [dict]

  @staticmethod
  def export_getFileTypes(in_dict):
    """
    It returns the file types for a given conditions.
    Input parameter is a dictionary which has the following keys:
    'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass','Production','RunNumber'
    """
    result = S_ERROR()
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    processing = in_dict.get('ProcessingPass', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    production = in_dict.get('Production', default)
    runnb = in_dict.get('RunNumber', default)
    visible = in_dict.get('Visible', 'Y')

    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    retVal = dataMGMT_.getFileTypes(configName, configVersion,
                                    conddescription, processing,
                                    evt, runnb,
                                    production, visible)
    if retVal['OK']:
      records = []
      parameters = ['FileTypes']
      for record in retVal['Value']:
        records += [[record[0]]]
      result = S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getStandardEventTypes = [dict]

  def export_getStandardEventTypes(self, in_dict):
    """more info in the BookkeepingClient.py"""
    self.export_getEventTypes(in_dict)

  #############################################################################
  def transfer_toClient(self, parameters, token, fileHelper):
    """ This method used to transfer data using a file. Currently two client methods are
    using this function: getFiles, getFilesWithMetadata
    """
    result = S_OK()
    iscPickleFormat = False
    try:
      in_dict = JEncoder.loads(parameters)
    except Exception as _:
      iscPickleFormat = True
      in_dict = cPickle.loads(parameters)
    gLogger.verbose("The following dictionary received:", "%" % in_dict)
    methodName = in_dict.get('MethodName', default)
    if methodName == 'getFiles':
      retVal = self.__getFiles(in_dict)
    else:
      retVal = self.__getFilesWithMetadata(in_dict)

    if iscPickleFormat:
      fileString = cPickle.dumps(retVal, protocol=2)
    else:
      fileString = JEncoder.dumps(retVal)

    retVal = fileHelper.stringToNetwork(fileString)
    if retVal['OK']:
      gLogger.debug('Sent file %s of size %d' % (in_dict, len(fileString)))
    else:
      gLogger.error("Failed to send files:", "%s" % in_dict)
      result = retVal
    return result

  #############################################################################
  @staticmethod
  @checkEnoughBKArguments
  def __getFiles(in_dict):
    """It returns a list of files.
    """
    simdesc = in_dict.get('SimulationConditions', default)
    datataking = in_dict.get('DataTakingConditions', default)
    procPass = in_dict.get('ProcessingPass', default)
    ftype = in_dict.get('FileType', default)
    evt = in_dict.get('EventType', default)
    configname = in_dict.get('ConfigName', default)
    configversion = in_dict.get('ConfigVersion', default)
    prod = in_dict.get('Production', in_dict.get('ProductionID', default))
    flag = in_dict.get('DataQuality', in_dict.get('DataQualityFlag', default))
    startd = in_dict.get('StartDate', None)
    endd = in_dict.get('EndDate', None)
    nbofevents = in_dict.get('NbOfEvents', False)
    startRunID = in_dict.get('StartRun', None)
    endRunID = in_dict.get('EndRun', None)
    runNbs = in_dict.get('RunNumber', in_dict.get('RunNumbers', []))
    if not isinstance(runNbs, list):
      runNbs = [runNbs]
    replicaFlag = in_dict.get('ReplicaFlag', 'Yes')
    visible = in_dict.get('Visible', default)
    filesize = in_dict.get('FileSize', False)
    tck = in_dict.get('TCK', [])

    if 'ProductionID' in in_dict:
      gLogger.verbose('ProductionID will be removed. It will changed to Production')

    if 'DataQualityFlag' in in_dict:
      gLogger.verbose('DataQualityFlag will be removed. It will changed to DataQuality')

    if 'RunNumbers' in in_dict:
      gLogger.verbose('RunNumbers will be removed. It will changed to RunNumbers')

    result = []
    retVal = dataMGMT_.getFiles(simdesc, datataking,
                                procPass, ftype, evt,
                                configname, configversion,
                                prod, flag, startd, endd,
                                nbofevents, startRunID,
                                endRunID, runNbs,
                                replicaFlag, visible,
                                filesize, tck)
    if not retVal['OK']:
      result = retVal
    else:
      values = retVal['Value']
      for i in values:
        result += [i[0]]
      result = S_OK(result)

    return result

  #############################################################################
  @staticmethod
  @checkEnoughBKArguments
  def __getFilesWithMetadata(in_dict):
    """
    It returns the files with their metadata. This result will be transfered to the client
    using a pickle file
    """
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    processing = in_dict.get('ProcessingPass', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    production = in_dict.get('Production', default)
    filetype = in_dict.get('FileType', default)
    quality = in_dict.get('DataQuality', in_dict.get('Quality', default))
    visible = in_dict.get('Visible', 'Y')
    replicaFlag = in_dict.get('ReplicaFlag', 'Yes')
    startDate = in_dict.get('StartDate', None)
    endDate = in_dict.get('EndDate', None)
    runnumbers = in_dict.get('RunNumber', in_dict.get('RunNumbers', []))
    startRunID = in_dict.get('StartRun', None)
    endRunID = in_dict.get('EndRun', None)
    tcks = in_dict.get('TCK')

    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in in_dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    retVal = dataMGMT_.getFilesWithMetadata(configName,
                                            configVersion,
                                            conddescription,
                                            processing,
                                            evt,
                                            production,
                                            filetype,
                                            quality,
                                            visible,
                                            replicaFlag,
                                            startDate,
                                            endDate,
                                            runnumbers,
                                            startRunID,
                                            endRunID,
                                            tcks)
    if retVal['OK']:
      records = []
      parameters = ['FileName', 'EventStat', 'FileSize',
                    'CreationDate', 'JobStart', 'JobEnd',
                    'WorkerNode', 'FileType', 'RunNumber',
                    'FillNumber', 'FullStat', 'DataqualityFlag',
                    'EventInputStat', 'TotalLuminosity', 'Luminosity',
                    'InstLuminosity', 'TCK', 'GUID', 'ADLER32', 'EventType', 'MD5SUM',
                    'VisibilityFlag', 'JobId', 'GotReplica', 'InsertTimeStamp']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2],
                     record[3], record[4], record[5],
                     record[6], record[7], record[8],
                     record[9], record[10], record[11],
                     record[12], record[13], record[14],
                     record[15], record[16], record[17], record[18], record[19],
                     record[20], record[21], record[22], record[23], record[24]]]
      retVal = {'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)}
    return retVal

  #############################################################################
  types_getFilesSumary = [dict]

  def export_getFilesSumary(self, in_dict):
    """more info in the BookkeepingClient.py"""
    return self.export_getFilesSummary(in_dict)

  #############################################################################
  types_getFilesSummary = [dict]

  @checkEnoughBKArguments
  def export_getFilesSummary(self, in_dict):
    """
    It returns sumary for a given data set.
    Input parameter is a dictionary which has the following keys:
    'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType',
    'ProcessingPass','Production','RunNumber', 'FileType', DataQuality
    """
    gLogger.debug('Input:', "%s" % in_dict)
    result = S_ERROR()

    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    condDescription = in_dict.get('ConditionDescription', default)
    processingPass = in_dict.get('ProcessingPass', default)
    eventType = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    production = in_dict.get('Production', default)
    fileType = in_dict.get('FileType', default)
    dataQuality = in_dict.get('DataQuality', in_dict.get('Quality', default))
    startRun = in_dict.get('StartRun', None)
    endRun = in_dict.get('EndRun', None)
    visible = in_dict.get('Visible', 'Y')
    startDate = in_dict.get('StartDate', None)
    endDate = in_dict.get('EndDate', None)
    runNumbers = in_dict.get('RunNumber', in_dict.get('RunNumbers', []))
    replicaFlag = in_dict.get('ReplicaFlag', 'Yes')
    tcks = in_dict.get('TCK')

    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in in_dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    retVal = dataMGMT_.getFilesSummary(configName=configName,
                                       configVersion=configVersion,
                                       conditionDescription=condDescription,
                                       processingPass=processingPass,
                                       eventType=eventType,
                                       production=production,
                                       fileType=fileType,
                                       dataQuality=dataQuality,
                                       startRun=startRun,
                                       endRun=endRun,
                                       visible=visible,
                                       startDate=startDate,
                                       endDate=endDate,
                                       runNumbers=runNumbers,
                                       replicaFlag=replicaFlag,
                                       tcks=tcks)
    if retVal['OK']:
      records = []
      parameters = ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2], record[3], record[4]]]
      result = S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      result = retVal

    return result

  #############################################################################
  types_getLimitedFiles = [dict]

  @staticmethod
  def export_getLimitedFiles(in_dict):
    """
    It returns a chunk of files. This method is equivalent to the getFiles.
    """
    result = S_ERROR()
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    processing = in_dict.get('ProcessingPass', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    production = in_dict.get('Production', default)
    filetype = in_dict.get('FileType', default)
    quality = in_dict.get('DataQuality', in_dict.get('Quality', default))
    runnb = in_dict.get('RunNumbers', in_dict.get('RunNumber', default))
    start = in_dict.get('StartItem', 0)
    maxValue = in_dict.get('MaxItem', 10)

    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in in_dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    retVal = dataMGMT_.getLimitedFiles(configName,
                                       configVersion,
                                       conddescription,
                                       processing,
                                       evt,
                                       production,
                                       filetype,
                                       quality,
                                       runnb,
                                       start,
                                       maxValue)
    if retVal['OK']:
      records = []
      parameters = ['Name', 'EventStat', 'FileSize',
                    'CreationDate', 'JobStart', 'JobEnd',
                    'WorkerNode', 'FileType', 'EventType',
                    'RunNumber', 'FillNumber', 'FullStat',
                    'DataqualityFlag', 'EventInputStat',
                    'TotalLuminosity', 'Luminosity',
                    'InstLuminosity', 'TCK']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2],
                     str(record[3]), str(record[4]),
                     str(record[5]), record[6], record[7],
                     record[8], record[9], record[10],
                     record[11], record[12], record[13],
                     record[14], record[15], record[16],
                     record[17]]]
      result = S_OK({'ParameterNames': parameters, 'Records': records, 'TotalRecords': len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableDataQuality = []

  @staticmethod
  def export_getAvailableDataQuality():
    """
    it returns all the available data quality flags.
    """
    return dataMGMT_.getAvailableDataQuality()

  #############################################################################
  types_getAvailableProductions = []

  @staticmethod
  def export_getAvailableProductions():
    """
    It returns all the available productions which have associated file with replica flag yes.
    """
    return dataMGMT_.getAvailableProductions()

  #############################################################################
  types_getAvailableRuns = []

  @staticmethod
  def export_getAvailableRuns():
    """
    It returns all the available runs which have associated files with reploica flag yes.
    """
    return dataMGMT_.getAvailableRuns()

  #############################################################################
  types_getAvailableEventTypes = []

  @staticmethod
  def export_getAvailableEventTypes():
    """
    It returns all the available event types.
    """
    return dataMGMT_.getAvailableEventTypes()

  #############################################################################
  types_getMoreProductionInformations = [int]

  @staticmethod
  def export_getMoreProductionInformations(prodid):
    """It returns inforation about a production"""
    return dataMGMT_.getMoreProductionInformations(prodid)

  #############################################################################
  types_getJobInfo = [basestring]

  @staticmethod
  def export_getJobInfo(lfn):
    """
    It returns the job metadata information for a given lfn produced by this job.
    """
    return dataMGMT_.getJobInfo(lfn)

  #############################################################################
  types_bulkJobInfo = [dict]

  @staticmethod
  def export_bulkJobInfo(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.bulkJobInfo(lfns)

  #############################################################################
  types_getJobInformation = [dict]

  @staticmethod
  def export_getJobInformation(in_dict):
    """
    It returns the job metadata information for a given lfn produced by this job.
    """
    return dataMGMT_.getJobInformation(in_dict)

  #############################################################################
  types_getRunNumber = [basestring]

  @staticmethod
  def export_getRunNumber(lfn):
    """
    It returns the run number for a given lfn!
    """
    return dataMGMT_.getRunNumber(lfn)

  #############################################################################
  types_getRunNbAndTck = [basestring]

  @staticmethod
  def export_getRunNbAndTck(lfn):
    """
    It returns the run number and tck for a given LFN.
    """
    return dataMGMT_.getRunNbAndTck(lfn)

  #############################################################################
  types_getProductionFiles = [(long, int), basestring]

  @staticmethod
  def export_getProductionFiles(prod, fileType, replica=default):
    """
    It returns files and their metadata for a given production, file type and replica.
    """
    return dataMGMT_.getProductionFiles(prod, fileType, replica)

  #############################################################################
  types_getAvailableRunNumbers = []

  def export_getAvailableRunNumbers(self):
    """more info in the BookkeepingClient.py"""
    return self.export_getAvailableRuns()

  #############################################################################
  types_getRunFiles = [int]

  @staticmethod
  def export_getRunFiles(runid):
    """
    It returns all the files and their metadata for a given run number!
    """
    return dataMGMT_.getRunFiles(runid)

  #############################################################################
  types_updateFileMetaData = [basestring, dict]

  @staticmethod
  def export_updateFileMetaData(filename, fileAttr):
    """
    This method used to modify files metadata.
    Input parametes is a stirng (filename) and a dictionary (fileAttr)
    with the file attributes. {'GUID':34826386286382,'EventStat':222222}
    """
    return dataMGMT_.updateFileMetaData(filename, fileAttr)

  #############################################################################
  types_renameFile = [basestring, basestring]

  @staticmethod
  def export_renameFile(oldLFN, newLFN):
    """
    It allows to change the name of a file which is in the Bookkeeping Metadata Catalogue.
    """
    return dataMGMT_.renameFile(oldLFN, newLFN)

  #############################################################################
  types_getProductionProcessingPassID = [(int, long)]

  @staticmethod
  def export_getProductionProcessingPassID(prodid):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionProcessingPassID(prodid)

  #############################################################################
  types_getProductionProcessingPass = [(long, int)]

  @staticmethod
  def export_getProductionProcessingPass(prodid):
    """
    It returns the processing pass for a given production.
    """
    return dataMGMT_.getProductionProcessingPass(prodid)

  #############################################################################
  types_insertTag = [dict]

  @staticmethod
  def export_insertTag(values):
    """
    It used to register tags (CONDB, DDDB, etc) to the database. The input parameter is dictionary: {'TagName':'Value'}
    """
    successfull = {}
    faild = {}

    for i in values:
      tags = values[i]
      for tag in tags:
        retVal = dataMGMT_.existsTag(i, tag)
        if retVal['OK'] and not retVal['Value']:
          retVal = dataMGMT_.insertTag(i, tag)
          if not retVal['OK']:
            faild[tag] = i
          else:
            successfull[tag] = i
        else:
          faild[tag] = i
    return S_OK({'Successfull': successfull, 'Faild': faild})

  #############################################################################
  types_setQuality = [list, basestring]

  def export_setQuality(self, lfns, flag):
    """more info in the BookkeepingClient.py"""
    return self.export_setFileDataQuality(lfns, flag)

  #############################################################################
  types_setFileDataQuality = [list, basestring]

  @staticmethod
  def export_setFileDataQuality(lfns, flag):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setFileDataQuality(lfns, flag)

  #############################################################################
  types_setRunAndProcessingPassDataQuality = [long, basestring, basestring]

  @staticmethod
  def export_setRunAndProcessingPassDataQuality(runNB, procpass, flag):
    """
    It sets the data quality to a run which belong to a given processing pass.
    This method insert a new row to the runquality table.
    This value used to set the data quality flag to a given run files which processed by a given processing pass.
    """
    return dataMGMT_.setRunAndProcessingPassDataQuality(runNB, procpass, flag)

  #############################################################################
  types_setRunQualityWithProcessing = [long, basestring, basestring]

  def export_setRunQualityWithProcessing(self, runNB, procpass, flag):
    """more info in the BookkeepingClient.py"""
    return self.export_setRunAndProcessingPassDataQuality(runNB, procpass, flag)

  #############################################################################
  types_setRunDataQuality = [int, basestring]

  @staticmethod
  def export_setRunDataQuality(runNb, flag):
    """
    It sets the data quality for a given run! The input parameter is the run number and a data quality flag.
    """
    return dataMGMT_.setRunDataQuality(runNb, flag)

  #############################################################################
  types_setQualityRun = [int, basestring]

  @staticmethod
  def export_setQualityRun(runNb, flag):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setRunDataQuality(runNb, flag)

  #############################################################################
  types_setProductionDataQuality = [int, basestring]

  @staticmethod
  def export_setProductionDataQuality(prod, flag):
    """
    It sets the data quality for a given production!
    """
    return dataMGMT_.setProductionDataQuality(prod, flag)

  #############################################################################
  types_setQualityProduction = [int, basestring]

  def export_setQualityProduction(self, prod, flag):
    """more info in the BookkeepingClient.py"""
    return self.export_setProductionDataQuality(prod, flag)

  types_getLFNsByProduction = [int]

  def export_getLFNsByProduction(self, prod):
    """more info in the BookkeepingClient.py"""
    return self.export_getProductionFiles(prod, 'ALL', 'ALL')

  #############################################################################
  types_getFileAncestors = [list, int, bool]

  @staticmethod
  def export_getFileAncestors(lfns, depth=None, replica=None):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileAncestors(lfns, depth, replica)

  #############################################################################
  types_getAllAncestors = [list, int]

  @staticmethod
  def export_getAllAncestors(lfns, depth):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = dataMGMT_.getFileAncestors(lfns, depth, False)
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [i['FileName'] for i in value]
      result = S_OK(values)
    else:
      result = retVal
    return result

  #############################################################################
  types_getAncestors = [list, int]

  @staticmethod
  def export_getAncestors(lfns, depth):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = dataMGMT_.getFileAncestors(lfns, depth, True)
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [i['FileName'] for i in value]
      result = S_OK(values)
    else:
      result = retVal
    return result

  #############################################################################
  types_getAllAncestorsWithFileMetaData = [list, int]

  @staticmethod
  def export_getAllAncestorsWithFileMetaData(lfns, depth):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileAncestors(lfns, depth, False)

  #############################################################################
  types_getAllDescendents = [list, int, int, bool]

  @staticmethod
  def export_getAllDescendents(lfn, depth=0, production=0, checkreplica=False):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileDescendents(lfn, depth, production, checkreplica)

  #############################################################################
  types_getDescendents = [list, int]

  def export_getDescendents(self, lfn, depth):
    """more info in the BookkeepingClient.py"""
    return self.export_getFileDescendants(lfn, depth)

  #############################################################################
  types_getFileDescendents = [list, int, int, bool]

  @staticmethod
  def export_getFileDescendents(lfn, depth, production=0, checkreplica=True):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileDescendents(lfn, depth, production, checkreplica)

  #############################################################################
  types_getFileDescendants = [list, int, int, bool]

  def export_getFileDescendants(self, lfn, depth, production=0, checkreplica=True):
    """more info in the BookkeepingClient.py"""
    return self.export_getFileDescendents(lfn, depth, production, checkreplica)

  #############################################################################
  types_checkfile = [basestring]

  @staticmethod
  def export_checkfile(fileName):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.checkfile(fileName)

  #############################################################################
  types_checkFileTypeAndVersion = [basestring, basestring]

  @staticmethod
  def export_checkFileTypeAndVersion(ftype, version):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.checkFileTypeAndVersion(ftype, version)

  #############################################################################
  types_checkEventType = [long]

  @staticmethod
  def export_checkEventType(eventTypeId):
    """more info in the BookkeepingClient.py"""
    if eventTypeId in __eventTypeCache:
      return __eventTypeCache[eventTypeId]
    else:
      retVal = dataMGMT_.checkEventType(eventTypeId)
      if retVal['OK']:
        __eventTypeCache[eventTypeId] = retVal
      else:
        return retVal
    return __eventTypeCache[eventTypeId]

  #############################################################################
  types_insertSimConditions = [dict]

  @staticmethod
  def export_insertSimConditions(in_dict):
    """
    It inserts a simulation condition to the Bookkeeping Metadata catalogue.
    """
    return dataMGMT_.insertSimConditions(in_dict)

  #############################################################################
  types_getSimConditions = []

  @staticmethod
  def export_getSimConditions():
    """
    It returns all the simulation conditions which are in the Bookkeeping Metadata catalogue.
    """
    return dataMGMT_.getSimConditions()

  #############################################################################
  types_removeReplica = [basestring]

  @staticmethod
  def export_removeReplica(fileName):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.removeReplica(fileName)

  #############################################################################
  types_getFileMetadata = [list]

  @staticmethod
  def export_getFileMetadata(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetadata(lfns)

  #############################################################################
  types_getFilesInformations = [list]

  @staticmethod
  def export_getFilesInformations(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetadata(lfns)

  #############################################################################
  types_getFileMetaDataForUsers = [list]

  @staticmethod
  def export_getFileMetaDataForUsers(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetaDataForWeb(lfns)

  #############################################################################
  types_getFileMetaDataForWeb = [list]

  @staticmethod
  def export_getFileMetaDataForWeb(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetaDataForWeb(lfns)

  #############################################################################
  types_getProductionFilesForUsers = [int, dict, dict, long, long]

  @staticmethod
  def export_getProductionFilesForUsers(prod, ftype, sortDict, startItem, maxitems):
    """more info in the BookkeepingClient.py"""
    res = dataMGMT_.getProductionFilesForWeb(prod, ftype, sortDict, startItem, maxitems)
    return res

  #############################################################################
  types_getProductionFilesForWeb = [long, dict, dict, long, long]

  @staticmethod
  def export_getProductionFilesWeb(prod, ftype, sortDict, startItem, maxitems):
    """
    It returns files and their metadata information for a given production.
    """
    return dataMGMT_.getProductionFilesForWeb(prod, ftype, sortDict, startItem, maxitems)

  #############################################################################
  types_exists = [list]

  @staticmethod
  def export_exists(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.exists(lfns)

  #############################################################################
  types_addReplica = [basestring]

  @staticmethod
  def export_addReplica(fileName):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.addReplica(fileName)

  #############################################################################
  types_getRunInformations = [long]

  @staticmethod
  def export_getRunInformations(runnb):
    """
    It returns run information and statistics.
    """
    return dataMGMT_.getRunInformations(runnb)

  #############################################################################
  types_getRunInformation = [dict]

  @staticmethod
  def export_getRunInformation(runnb):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunInformation(runnb)

  #############################################################################
  types_getFileCreationLog = [basestring]

  @staticmethod
  def export_getFileCreationLog(lfn):
    """
    For a given file returns the log files of the job which created it.
    """
    return dataMGMT_.getFileCreationLog(lfn)

  #############################################################################
  types_getLogfile = [basestring]

  @staticmethod
  def export_getLogfile(lfn):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileCreationLog(lfn)

  #############################################################################
  types_insertEventType = [long, basestring, basestring]

  @staticmethod
  def export_insertEventType(evid, desc, primary):
    """
    It inserts an event type to the Bookkeeping Metadata catalogue.
    """
    result = S_ERROR()

    retVal = dataMGMT_.checkEventType(evid)
    if not retVal['OK']:
      retVal = dataMGMT_.insertEventTypes(evid, desc, primary)
      if retVal['OK']:
        result = S_OK(str(evid) + ' event type added successfully!')
      else:
        result = retVal
    else:
      result = S_OK(str(evid) + ' event type exists')
    return result

  #############################################################################
  types_addEventType = [long, basestring, basestring]

  def export_addEventType(self, evid, desc, primary):
    """more info in the BookkeepingClient.py"""
    return self.export_insertEventType(evid, desc, primary)

  #############################################################################
  types_updateEventType = [long, basestring, basestring]

  @staticmethod
  def export_updateEventType(evid, desc, primary):
    """
    It can used to modify an existing event type.
    """
    result = S_ERROR()

    retVal = dataMGMT_.checkEventType(evid)
    if not retVal['OK']:
      result = S_ERROR(str(evid) + ' event type is missing in the BKK database!')
    else:
      retVal = dataMGMT_.updateEventType(evid, desc, primary)
      if retVal['OK']:
        result = S_OK(str(evid) + ' event type updated successfully!')
      else:
        result = retVal
    return result

  #############################################################################
  types_addFiles = [list]

  @staticmethod
  def export_addFiles(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.addReplica(lfns)

  #############################################################################
  types_removeFiles = [list]

  @staticmethod
  def export_removeFiles(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.removeReplica(lfns)

  #############################################################################
  types_getProductionSummary = [dict]

  @staticmethod
  def export_getProductionSummary(in_dict):
    """
    It can used to count the number of events for a given dataset.
    """

    cName = in_dict.get('ConfigName', default)
    cVersion = in_dict.get('ConfigVersion', default)
    production = in_dict.get('Production', default)
    simdesc = in_dict.get('ConditionDescription', default)
    pgroup = in_dict.get('ProcessingPass', default)
    ftype = in_dict.get('FileType', default)
    evttype = in_dict.get('EventType', default)
    retVal = dataMGMT_.getProductionSummary(cName, cVersion, simdesc, pgroup, production, ftype, evttype)

    return retVal

  #############################################################################
  types_getProductionInformations_new = [long]

  def export_getProductionInformations_new(self, prodid):
    """more info in the BookkeepingClient.py"""
    return self.export_getProductionInformations(prodid)

  #############################################################################
  types_getProductionInformations = [(long, int)]

  @staticmethod
  def export_getProductionInformations(prodid):
    """
    It returns a statistic (data processing phases, number of events, etc.) for a given production.
    """

    nbjobs = None
    nbOfFiles = None
    nbOfEvents = None
    steps = None
    prodinfos = None

    value = dataMGMT_.getProductionNbOfJobs(prodid)
    if value['OK']:
      nbjobs = value['Value']

    value = dataMGMT_.getProductionNbOfFiles(prodid)
    if value['OK']:
      nbOfFiles = value['Value']

    value = dataMGMT_.getProductionNbOfEvents(prodid)
    if value['OK']:
      nbOfEvents = value['Value']

    value = dataMGMT_.getConfigsAndEvtType(prodid)
    if value['OK']:
      prodinfos = value['Value']

    path = '/'

    if len(prodinfos) == 0:
      return S_ERROR('This production does not contains any jobs!')
    cname = prodinfos[0][0]
    cversion = prodinfos[0][1]
    path += cname + '/' + cversion + '/'

    value = dataMGMT_.getSteps(prodid)
    if value['OK']:
      steps = value['Value']
    else:
      steps = value['Message']
      result = {"Production informations": prodinfos,
                "Steps": steps,
                "Number of jobs": nbjobs,
                "Number of files": nbOfFiles,
                "Number of events": nbOfEvents,
                'Path': path}
      return S_OK(result)

      # return S_ERROR(value['Message'])

    res = dataMGMT_.getProductionSimulationCond(prodid)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      path += res['Value']
    res = dataMGMT_.getProductionProcessingPass(prodid)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      path += res['Value']
    prefix = '\n' + path

    for i in nbOfEvents:
      path += prefix + '/' + str(i[2]) + '/' + i[0]
    result = {"Production informations": prodinfos,
              "Steps": steps,
              "Number of jobs": nbjobs,
              "Number of files": nbOfFiles,
              "Number of events": nbOfEvents,
              'Path': path}
    return S_OK(result)

  #############################################################################
  types_getProductionInformationsFromView = [long]

  @staticmethod
  def export_getProductionInformationsFromView(prodid):
    """
    It is exactly same as getProductionInformations, but it much faster. The result is in the materialized view.
    """

    result = S_ERROR()
    value = dataMGMT_.getProductionInformationsFromView(prodid)
    parameters = []
    infos = []
    if value['OK']:
      records = value['Value']
      parameters = ['Production', 'EventType', 'FileType', 'NumberOfEvents', 'NumberOfFiles']
      for record in records:
        infos += [[record[0], record[1], record[2], record[3], record[4]]]
      result = S_OK({'ParameterNames': parameters, 'Records': infos})
    else:
      result = value
    return result

  #############################################################################
  types_getFileHistory = [basestring]

  @staticmethod
  def export_getFileHistory(lfn):
    """
    It returns all the information about a file.
    """
    retVal = dataMGMT_.getFileHistory(lfn)
    result = {}
    records = []
    if retVal['OK']:
      values = retVal['Value']
      parameterNames = ['FileId', 'FileName',
                        'ADLER32', 'CreationDate',
                        'EventStat', 'Eventtype',
                        'Gotreplica', 'GUI', 'JobId',
                        'md5sum', 'FileSize', 'FullStat',
                        'Dataquality', 'FileInsertDate',
                        'Luminosity', 'InstLuminosity']
      counter = 0
      for record in values:
        value = [record[0], record[1], record[2],
                 record[3], record[4], record[5],
                 record[6], record[7], record[8],
                 record[9], record[10], record[11],
                 record[12], record[13], record[14],
                 record[15]]
        records += [value]
        counter += 1
      result = {'ParameterNames': parameterNames, 'Records': records, 'TotalRecords': counter}
    else:
      result = S_ERROR(retVal['Message'])
    return S_OK(result)

  #############################################################################
  types_getJobsNb = [long]

  @staticmethod
  def export_getJobsNb(prodid):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfJobs(prodid)

  #############################################################################
  types_getProductionNbOfJobs = [long]

  @staticmethod
  def export_getProductionNbOfJobs(prodid):
    """
    It returns the number of jobs for a given production.
    """
    return dataMGMT_.getProductionNbOfJobs(prodid)

  #############################################################################
  types_getNumberOfEvents = [long]

  @staticmethod
  def export_getNumberOfEvents(prodid):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfEvents(prodid)

  #############################################################################
  types_getProductionNbOfEvents = [long]

  @staticmethod
  def export_getProductionNbOfEvents(prodid):
    """
    It returns the number of events for a given production.
    """
    return dataMGMT_.getProductionNbOfEvents(prodid)

  #############################################################################
  types_getSizeOfFiles = [long]

  @staticmethod
  def export_getSizeOfFiles(prodid):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionSizeOfFiles(prodid)

  #############################################################################
  types_getProductionSizeOfFiles = [long]

  @staticmethod
  def export_getProductionSizeOfFiles(prodid):
    """
    It returns the size of files for a given production.
    """
    return dataMGMT_.getProductionSizeOfFiles(prodid)

  #############################################################################
  types_getNbOfFiles = [long]

  @staticmethod
  def export_getNbOfFiles(prodid):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfFiles(prodid)

  #############################################################################
  types_getProductionNbOfFiles = [long]

  @staticmethod
  def export_getProductionNbOfFiles(prodid):
    """
    It returns the number of files produced by a given production.
    """
    return dataMGMT_.getProductionNbOfFiles(prodid)

  #############################################################################
  types_getProductionInformation = [long]

  @staticmethod
  def export_getProductionInformation(prodid):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionInformation(prodid)

  #############################################################################
  types_getNbOfJobsBySites = [long]

  @staticmethod
  def export_getNbOfJobsBySites(prodid):
    """
    It returns the number of jobs executed at different sites for a given production.
    """
    return dataMGMT_.getNbOfJobsBySites(prodid)

  #############################################################################
  types_getAvailableTags = []

  @staticmethod
  def export_getAvailableTags():
    """
    It returns the available database tags.
    """
    return dataMGMT_.getAvailableTags()

  #############################################################################
  types_getProcessedEvents = [(long, int)]

  @staticmethod
  def export_getProcessedEvents(prodid):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionProcessedEvents(prodid)

  #############################################################################
  types_getProductionProcessedEvents = [(long, int)]

  @staticmethod
  def export_getProductionProcessedEvents(prodid):
    """
    it returns the number of events processed for a given production.
    """
    gLogger.debug('getProductionProcessedEvents->Production:', '%d ' % prodid)
    return dataMGMT_.getProductionProcessedEvents(prodid)

  #############################################################################
  types_getRunsForAGivenPeriod = [dict]

  @staticmethod
  def export_getRunsForAGivenPeriod(in_dict):
    """
    It returns the available runs between a period.
    Input parameters:
    AllowOutsideRuns: If it is true, it only returns the runs which finished before EndDate.
    StartDate: the run start period
    EndDate: the run end period
    CheckRunStatus: if it is true, it check the run is processed or not processed.
    """
    return dataMGMT_.getRunsForAGivenPeriod(in_dict)

  #############################################################################
  types_getProductiosWithAGivenRunAndProcessing = [dict]

  def export_getProductiosWithAGivenRunAndProcessing(self, in_dict):
    """more info in the BookkeepingClient.py"""
    return self.export_getProductionsFromView(in_dict)

  #############################################################################
  types_getProductionsFromView = [dict]

  @staticmethod
  def export_getProductionsFromView(in_dict):
    """
    It returns the productions from the bookkeeping view for a given processing pass and run number.
    Input parameters:
    RunNumber
    ProcessingPass
    """
    return dataMGMT_.getProductionsFromView(in_dict)

  #############################################################################
  types_getDataQualityForRuns = [list]

  def export_getDataQualityForRuns(self, runs):
    """more info in the BookkeepingClient.py"""
    return self.export_getRunFilesDataQuality(runs)

  #############################################################################
  types_getRunFilesDataQuality = [list]

  @staticmethod
  def export_getRunFilesDataQuality(runs):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunFilesDataQuality(runs)

  #############################################################################
  types_setFilesInvisible = [list]

  @staticmethod
  def export_setFilesInvisible(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setFilesInvisible(lfns)

  #############################################################################
  types_setFilesVisible = [list]

  @staticmethod
  def export_setFilesVisible(lfns):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setFilesVisible(lfns)

  #############################################################################
  types_getRunFlag = [long, long]

  def export_getRunFlag(self, runnb, processing):
    """more info in the BookkeepingClient.py"""
    return self.export_getRunAndProcessingPassDataQuality(runnb, processing)

  #############################################################################
  types_getRunAndProcessingPassDataQuality = [long, long]

  @staticmethod
  def export_getRunAndProcessingPassDataQuality(runnb, processing):
    """
    It returns the data quality flag for a given run and processing pass.
    """
    return dataMGMT_.getRunAndProcessingPassDataQuality(runnb, processing)

  #############################################################################
  types_getAvailableConfigurations = []

  @staticmethod
  def export_getAvailableConfigurations():
    """
    It returns the available configurations.
    """
    return dataMGMT_.getAvailableConfigurations()

  #############################################################################
  types_getRunProcessingPass = [long]

  @staticmethod
  def export_getRunProcessingPass(runnumber):
    """
    it returns the run number for a given run.
    """
    return dataMGMT_.getRunProcessingPass(runnumber)

  #############################################################################
  types_getProductionFilesStatus = [int, list]

  @staticmethod
  def export_getProductionFilesStatus(productionid=None, lfns=None):
    """
    It returns the file status in the bkk for a given production or a list of lfns.
    """
    if not lfns:
      lfns = []
    return dataMGMT_.getProductionFilesStatus(productionid, lfns)

  #############################################################################
  types_getFilesWithGivenDataSets = [dict]

  def export_getFilesWithGivenDataSets(self, values):
    """more info in the BookkeepingClient.py"""
    gLogger.debug('getFiles dataset:', "%s" % values)
    return self.export_getFiles(values)

  #############################################################################
  types_getFiles = [dict]

  @staticmethod
  def export_getFiles(values):
    """more info in the BookkeepingClient.py"""

    simdesc = values.get('SimulationConditions', default)
    datataking = values.get('DataTakingConditions', default)
    procPass = values.get('ProcessingPass', default)
    ftype = values.get('FileType', default)
    evt = values.get('EventType', 0)
    configname = values.get('ConfigName', default)
    configversion = values.get('ConfigVersion', default)
    prod = values.get('Production', values.get('ProductionID', default))
    flag = values.get('DataQuality', values.get('DataQualityFlag', default))
    startd = values.get('StartDate', None)
    endd = values.get('EndDate', None)
    nbofevents = values.get('NbOfEvents', False)
    startRunID = values.get('StartRun', None)
    endRunID = values.get('EndRun', None)
    runNbs = values.get('RunNumber', values.get('RunNumbers', []))
    if not isinstance(runNbs, list):
      runNbs = [runNbs]
    replicaFlag = values.get('ReplicaFlag', 'Yes')
    visible = values.get('Visible', default)
    filesize = values.get('FileSize', False)
    tck = values.get('TCK')

    if 'ProductionID' in values:
      gLogger.verbose('ProductionID will be removed. It will changed to Production')

    if 'DataQualityFlag' in values:
      gLogger.verbose('DataQualityFlag will be removed. It will changed to DataQuality')

    if 'RunNumbers' in values:
      gLogger.verbose('RunNumbers will be removed. It will changed to RunNumbers')

    result = []
    retVal = dataMGMT_.getFiles(simdesc, datataking,
                                procPass, ftype, evt,
                                configname, configversion,
                                prod, flag, startd, endd,
                                nbofevents, startRunID,
                                endRunID, runNbs,
                                replicaFlag, visible,
                                filesize, tck)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      values = retVal['Value']
      for i in values:
        result += [i[0]]

    return S_OK(result)

  #############################################################################
  types_getFilesWithGivenDataSetsForUsers = [dict]

  def export_getFilesWithGivenDataSetsForUsers(self, values):
    """more info in the BookkeepingClient.py"""
    return self.export_getVisibleFilesWithMetadata(values)

  #############################################################################
  types_getVisibleFilesWithMetadata = [dict]

  @staticmethod
  def export_getVisibleFilesWithMetadata(in_dict):
    """
    It returns a list of files with metadata for a given condition.
    """

    conddescription = in_dict.get('SimulationConditions', in_dict.get('DataTakingConditions', default))
    procPass = in_dict.get('ProcessingPass', default)
    ftype = in_dict.get('FileType', default)
    evt = in_dict.get('EventType', default)
    configname = in_dict.get('ConfigName', default)
    configversion = in_dict.get('ConfigVersion', default)
    prod = in_dict.get('Production', in_dict.get('ProductionID', default))
    dqflag = in_dict.get('DataQuality', in_dict.get('DataQualityFlag', default))
    startd = in_dict.get('StartDate', None)
    endd = in_dict.get('EndDate', None)
    startRunID = in_dict.get('StartRun', None)
    endRunID = in_dict.get('EndRun', None)
    runNbs = in_dict.get('RunNumber', in_dict.get('RunNumbers', []))
    replicaFlag = in_dict.get('ReplicaFlag', 'Yes')
    tck = in_dict.get('TCK', [])
    visible = in_dict.get('Visible', 'Y')

    if ftype == default:
      return S_ERROR('FileType is missing!')

    if 'ProductionID' in in_dict:
      gLogger.verbose('ProductionID will be removed. It will changed to Production')

    if 'DataQualityFlag' in in_dict:
      gLogger.verbose('DataQualityFlag will be removed. It will changed to DataQuality')

    if 'RunNumbers' in in_dict:
      gLogger.verbose('RunNumbers will be removed. It will changed to RunNumbers')

    gLogger.debug("getVisibleFilesWithMetadata->", "%s" % in_dict)
    result = {}
    retVal = dataMGMT_.getFilesWithMetadata(configName=configname,
                                            configVersion=configversion,
                                            conddescription=conddescription,
                                            processing=procPass,
                                            evt=evt,
                                            production=prod,
                                            filetype=ftype,
                                            quality=dqflag,
                                            visible=visible,
                                            replicaflag=replicaFlag,
                                            startDate=startd,
                                            endDate=endd,
                                            runnumbers=runNbs,
                                            startRunID=startRunID,
                                            endRunID=endRunID,
                                            tcks=tck)

    summary = 0

    parameters = ['FileName', 'EventStat', 'FileSize',
                  'CreationDate', 'JobStart', 'JobEnd',
                  'WorkerNode', 'FileType', 'RunNumber',
                  'FillNumber', 'FullStat', 'DataqualityFlag',
                  'EventInputStat', 'TotalLuminosity', 'Luminosity',
                  'InstLuminosity', 'TCK', 'GUID', 'ADLER32', 'EventType', 'MD5SUM',
                  'VisibilityFlag', 'JobId', 'GotReplica', 'InsertTimeStamp']

    if not retVal['OK']:
      return retVal
    else:
      values = retVal['Value']
      nbfiles = 0
      nbevents = 0
      evinput = 0
      fsize = 0
      tLumi = 0
      lumi = 0
      ilumi = 0
      for i in values:
        nbfiles = nbfiles + 1
        row = dict(zip(parameters, i))
        if row['EventStat'] != None:
          nbevents += row['EventStat']
        if row['EventInputStat'] != None:
          evinput += row['EventInputStat']
        if row['FileSize'] != None:
          fsize += row['FileSize']
        if row['TotalLuminosity'] != None:
          tLumi += row['TotalLuminosity']
        if row['Luminosity'] != None:
          lumi += row['Luminosity']
        if row['InstLuminosity'] != None:
          ilumi += row['InstLuminosity']
        result[row['FileName']] = {'EventStat': row['EventStat'],
                                   'EventInputStat': row['EventInputStat'],
                                   'Runnumber': row['RunNumber'],
                                   'Fillnumber': row['FillNumber'],
                                   'FileSize': row['FileSize'],
                                   'TotalLuminosity': row['TotalLuminosity'],
                                   'Luminosity': row['Luminosity'],
                                   'InstLuminosity': row['InstLuminosity'],
                                   'TCK': row['TCK']}
      if nbfiles > 0:
        summary = {'Number Of Files': nbfiles,
                   'Number of Events': nbevents,
                   'EventInputStat': evinput,
                   'FileSize': fsize / 1000000000.,
                   'TotalLuminosity': tLumi,
                   'Luminosity': lumi,
                   'InstLuminosity': ilumi}
    return S_OK({'LFNs': result, 'Summary': summary})

  #############################################################################
  types_addProduction = [dict]

  @staticmethod
  def export_addProduction(infos):
    """
    It is used to register a production to the bkk.
    Input parameters:
    SimulationConditions
    DataTakingConditions
    Steps: the step which is used to process data for a given production.
    Production:
    InputProductionTotalProcessingPass: it is a path of the input data processing pass
    """

    gLogger.debug("%s" % infos)
    result = S_OK()
    simcond = infos.get('SimulationConditions', None)
    daqdesc = infos.get('DataTakingConditions', None)
    production = None

    if simcond is None and daqdesc is None:
      result = S_ERROR('SimulationConditions and DataTakingConditions are both missing!')

    if 'Steps' not in infos:
      result = S_ERROR("Missing Steps!")
    if 'Production' not in infos:
      result = S_ERROR('Production is missing!')
    if 'EventType' not in infos:
      result = S_ERROR("EventType is missing!")

    if result['OK']:
      steps = infos['Steps']
      inputProdTotalProcessingPass = ''
      production = infos['Production']
      inputProdTotalProcessingPass = infos.get('InputProductionTotalProcessingPass', '')
      configName = infos.get("ConfigName")
      configVersion = infos.get("ConfigVersion")
      eventType = infos.get("EventType")
      result = dataMGMT_.addProduction(production=production,
                                       simcond=simcond,
                                       daq=daqdesc,
                                       steps=steps,
                                       inputproc=inputProdTotalProcessingPass,
                                       configName=configName,
                                       configVersion=configVersion,
                                       eventType=eventType)
    return result

  #############################################################################
  types_getEventTypes = [dict]

  @staticmethod
  def export_getEventTypes(in_dict):
    """
    It returns the available event types for a given configuration name and configuration version.
    Input parameters:
    ConfigName, ConfigVersion, Production
    """

    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    production = in_dict.get('Production', default)
    return dataMGMT_.getEventTypes(configName, configVersion, production)

  #############################################################################
  types_getProcessingPassSteps = [dict]

  @staticmethod
  def export_getProcessingPassSteps(in_dict):
    """
    It returns the steps for a given stepname processing pass ands production.
    """
    stepname = in_dict.get('StepName', default)
    cond = in_dict.get('ConditionDescription', default)
    procpass = in_dict.get('ProcessingPass', default)

    return dataMGMT_.getProcessingPassSteps(procpass, cond, stepname)

  #############################################################################
  types_getProductionProcessingPassSteps = [dict]

  @staticmethod
  def export_getProductionProcessingPassSteps(in_dict):
    """
    it returns the steps for a given production.
    """

    if 'Production' in in_dict:
      return dataMGMT_.getProductionProcessingPassSteps(in_dict['Production'])
    else:
      return S_ERROR('The Production dictionary key is missing!!!')

  #############################################################################
  types_getProductionOutputFiles = [dict]

  def export_getProductionOutputFiles(self, in_dict):
    """more info in the BookkeepingClient.py"""

    return self.export_getProductionOutputFileTypes(in_dict)

  #############################################################################
  types_getProductionOutputFileTypes = [dict]

  @staticmethod
  def export_getProductionOutputFileTypes(in_dict):
    """
    It returns the output file types which produced by a given production.
    """

    production = in_dict.get('Production', default)
    stepid = in_dict.get('StepId', default)

    if production != default:
      return dataMGMT_.getProductionOutputFileTypes(production, stepid)
    else:
      return S_ERROR('The Production dictionary key is missing!!!')

  #############################################################################
  types_getRunQuality = [basestring, basestring]

  def export_getRunQuality(self, procpass, flag=default):
    """more info in the BookkeepingClient.py"""

    return self.export_getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  types_getRunWithProcessingPassAndDataQuality = [basestring, basestring]

  @staticmethod
  def export_getRunWithProcessingPassAndDataQuality(procpass, flag=default):
    """
    It returns the run number for a given processing pass and a flag from the run quality table.
    """
    return dataMGMT_.getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  types_getRuns = [dict]

  @staticmethod
  def export_getRuns(in_dict):
    """
    It returns the runs for a given configuration name and version.
    Input parameters:

    """
    result = S_ERROR()
    cName = in_dict.get('ConfigName', default)
    cVersion = in_dict.get('ConfigVersion', default)
    if cName != default and cVersion != default:
      result = dataMGMT_.getRuns(cName, cVersion)
    else:
      result = S_ERROR('The configuration name and version have to be defined!')
    return result

  #############################################################################
  types_getRunProcPass = [dict]

  def export_getRunProcPass(self, in_dict):
    """more info in the BookkeepingClient.py"""
    return self.export_getRunAndProcessingPass(in_dict)

  #############################################################################
  types_getRunAndProcessingPass = [dict]

  @staticmethod
  def export_getRunAndProcessingPass(in_dict):
    """
    It returns all the processing pass and run number for a given run.
    """
    run = in_dict.get('RunNumber', default)
    result = S_ERROR()
    if run != default:
      result = dataMGMT_.getRunAndProcessingPass(run)
    else:
      result = S_ERROR('The run number has to be specified!')
    return result

  #############################################################################
  types_getProcessingPassId = [basestring]

  @staticmethod
  def export_getProcessingPassId(fullpath):
    """
    It returns the ProcessingPassId for a given path. this method should not used!
    """
    return dataMGMT_.getProcessingPassId(fullpath)

  #############################################################################
  types_getRunNbFiles = [dict]

  def export_getRunNbFiles(self, in_dict):
    """more info in the BookkeepingClient.py"""
    return self.export_getNbOfRawFiles(in_dict)

  #############################################################################
  types_getNbOfRawFiles = [dict]

  @staticmethod
  def export_getNbOfRawFiles(in_dict):
    """
    It counts the raw files for a given run and (or) event type.
    """

    runnb = in_dict.get('RunNumber', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    replicaFlag = in_dict.get('ReplicaFlag', 'Yes')
    visible = in_dict.get('Visible', 'Y')
    isFinished = in_dict.get("Finished", 'ALL')
    result = S_ERROR()
    if runnb == default and evt == default:
      result = S_ERROR('Run number or event type must be given!')
    else:
      retVal = dataMGMT_.getNbOfRawFiles(runnb, evt, replicaFlag, visible, isFinished)
      if retVal['OK']:
        result = S_OK(retVal['Value'][0][0])
      else:
        result = retVal
    return result

  #############################################################################
  types_getTypeVersion = [list]

  def export_getTypeVersion(self, lfn):
    """more info in the BookkeepingClient.py"""
    return self.export_getFileTypeVersion(lfn)

  #############################################################################
  types_getFileTypeVersion = [list]

  @staticmethod
  def export_getFileTypeVersion(lfn):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileTypeVersion(lfn)

  #############################################################################
  types_getTCKs = [dict]

  @staticmethod
  def export_getTCKs(in_dict):
    """
    It returns the tcks for a given data set.
    """
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    processing = in_dict.get('ProcessingPass', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    production = in_dict.get('Production', default)
    filetype = in_dict.get('FileType', default)
    quality = in_dict.get('DataQuality', in_dict.get('Quality', default))
    runnb = in_dict.get('RunNumber', default)
    result = S_ERROR()
    if 'Quality' in in_dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    retVal = dataMGMT_.getTCKs(configName,
                               configVersion,
                               conddescription,
                               processing,
                               evt,
                               production,
                               filetype,
                               quality,
                               runnb)
    if retVal['OK']:
      records = []
      for record in retVal['Value']:
        records += [record[0]]
      result = S_OK(records)
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableTcks = [dict]

  def export_getAvailableTcks(self, in_dict):
    """more info in the BookkeepingClient.py"""
    return self.export_getTCKs(in_dict)

  #############################################################################
  types_getStepsMetadata = [dict]

  @staticmethod
  def export_getStepsMetadata(in_dict):
    """
    It returns the step(s) which is produced  a given dataset.
    """
    gLogger.debug('getStepsMetadata', "%s" % in_dict)
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    cond = in_dict.get('ConditionDescription', default)
    procpass = in_dict.get('ProcessingPass', default)
    evt = in_dict.get('EventType', in_dict.get('EventTypeId', default))
    production = in_dict.get('Production', default)
    filetype = in_dict.get('FileType', default)
    runnb = in_dict.get('RunNumber', default)

    if 'EventTypeId' in in_dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in in_dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    return dataMGMT_.getStepsMetadata(configName, configVersion, cond, procpass, evt, production, filetype, runnb)

  #############################################################################
  types_getDirectoryMetadata_new = [list]

  @staticmethod
  def export_getDirectoryMetadata_new(lfn):
    """more info in the BookkeepingClient.py"""
    gLogger.verbose("Getting the metadata for:", "%s" % lfn)
    return dataMGMT_.getDirectoryMetadata(lfn)

    #############################################################################
  types_getDirectoryMetadata = [list]

  @staticmethod
  def export_getDirectoryMetadata(lfn):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getDirectoryMetadata(lfn)

  #############################################################################
  types_getFilesForGUID = [basestring]

  @staticmethod
  def export_getFilesForGUID(guid):
    """
    It returns a file for a given GUID.
    """
    return dataMGMT_.getFilesForGUID(guid)

  #############################################################################
  types_getRunsGroupedByDataTaking = []

  @staticmethod
  def export_getRunsGroupedByDataTaking():
    """
    It returns all the run numbers grouped by the data taking description.
    """
    return dataMGMT_.getRunsGroupedByDataTaking()

  #############################################################################
  types_getListOfFills = [dict]

  @staticmethod
  def export_getListOfFills(in_dict):
    """
    It returns a list of FILL numbers for a given Configuration name,
    Configuration version and data taking description.
    """
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    return dataMGMT_.getListOfFills(configName, configVersion, conddescription)

  #############################################################################
  types_getRunsForFill = [long]

  @staticmethod
  def export_getRunsForFill(fillid):
    """
    It returns a list of runs for a given FILL
    """
    return dataMGMT_.getRunsForFill(fillid)

  #############################################################################
  types_getListOfRuns = [dict]

  @staticmethod
  def export_getListOfRuns(in_dict):
    """
    It returns a list of runs for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion',
    'ConditionDescription', 'EventType','ProcessingPass'
    """
    result = S_ERROR()
    configName = in_dict.get('ConfigName', default)
    configVersion = in_dict.get('ConfigVersion', default)
    conddescription = in_dict.get('ConditionDescription', default)
    processing = in_dict.get('ProcessingPass', default)
    evt = in_dict.get('EventType', default)
    quality = in_dict.get('DataQuality', default)

    retVal = dataMGMT_.getListOfRuns(configName, configVersion, conddescription, processing, evt, quality)
    if not retVal['OK']:
      result = retVal
    else:
      result = S_OK([i[0] for i in retVal['Value']])
    return result

  #############################################################################
  types_getSimulationConditions = [dict]

  @staticmethod
  def export_getSimulationConditions(in_dict):
    """It returns a list of simulation conditions for a given conditions
    """
    return dataMGMT_.getSimulationConditions(in_dict)

  #############################################################################
  types_updateSimulationConditions = [dict]

  @staticmethod
  def export_updateSimulationConditions(in_dict):
    """It updates a given simulation condition
    """
    return dataMGMT_.updateSimulationConditions(in_dict)

  #############################################################################
  types_deleteSimulationConditions = [long]

  @staticmethod
  def export_deleteSimulationConditions(simid):
    """deletes a given simulation conditions"""
    return dataMGMT_.deleteSimulationConditions(simid)

  #############################################################################
  types_getProductionSummaryFromView = [dict]

  @staticmethod
  def export_getProductionSummaryFromView(in_dict):
    """it returns a summary for a given condition."""
    return dataMGMT_.getProductionSummaryFromView(in_dict)

  types_getJobInputOutputFiles = [list]

  @staticmethod
  def export_getJobInputOutputFiles(diracjobids):
    """It returns the input and output files for a given DIRAC jobid"""
    return dataMGMT_.getJobInputOutputFiles(diracjobids)

  types_setRunOnlineFinished = [long]

  @staticmethod
  def export_setRunOnlineFinished(runnumber):
    "It is used to set the run finished..."
    return dataMGMT_.setRunStatusFinished(runnumber, 'Y')

  types_setRunOnlineNotFinished = [long]

  @staticmethod
  def export_setRunOnlineNotFinished(runnumber):
    """You can set the runs not finished"""
    return dataMGMT_.setRunStatusFinished(runnumber, 'N')

  types_getRunStatus = [list]

  @staticmethod
  def export_getRunStatus(runnumbers):
    """ it returns the status of the runs"""
    return dataMGMT_.getRunStatus(runnumbers)

  types_bulkupdateFileMetaData = [dict]

  @staticmethod
  def export_bulkupdateFileMetaData(lfnswithmeta):
    """
    It updates the file metadata
    """
    return dataMGMT_.bulkupdateFileMetaData(lfnswithmeta)

  types_fixRunLuminosity = [list]

  @staticmethod
  def export_fixRunLuminosity(runnumbers):
    return dataMGMT_.fixRunLuminosity(runnumbers)

  #############################################################################
  types_getProductionProducedEvents = [(long, int)]

  @staticmethod
  def export_getProductionProducedEvents(prodid):
    """
    it returns the number of events producced for a given production.
    """
    gLogger.verbose("Retrieving the number of processed event for production", prodid)
    return dataMGMT_.getProductionProducedEvents(prodid)

  #############################################################################
  types_bulkinsertEventType = [list]

  @staticmethod
  def export_bulkinsertEventType(eventtypes):
    """
    It inserts a list of event types to the db.

    :param list eventtypes it is a list of event types. For example: the list elements are the following:
    {'EVTTYPEID': '12265021', 'DESCRIPTION': 'Bu_D0pipipi,Kpi-withf2=DecProdCut_pCut1600MeV',
    'PRIMARY': '[B+ -> (D~0 -> K+ pi-) pi+ pi- pi+]cc'}
    :return S_ERROR S_OK({'Failed':[],'Successful':[]})
    """
    return dataMGMT_.bulkinsertEventType(eventtypes)

  #############################################################################
  types_bulkupdateEventType = [list]

  @staticmethod
  def export_bulkupdateEventType(eventtypes):
    """
    It updates a list of event types which are exist in the db

    :param list eventtypes it is a list of event types. For example: the list elements are the following:
    {'EVTTYPEID': '12265021', 'DESCRIPTION':
    'Bu_D0pipipi,Kpi-withf2=DecProdCut_pCut1600MeV', 'PRIMARY': '[B+ -> (D~0 -> K+ pi-) pi+ pi- pi+]cc'}
    :return S_ERROR S_OK({'Failed':[],'Successful':[]})
    """
    return dataMGMT_.bulkupdateEventType(eventtypes)

  #############################################################################
  types_getRunConfigurationsAndDataTakingCondition = [long]

  @staticmethod
  def export_getRunConfigurationsAndDataTakingCondition(runnumber):
    """
    It returns minimal information for a given run.
    :param: int runnumber
    :return: S_OK()/S_ERROR ConfigName, ConfigVersion and DataTakingDescription
    """
    return dataMGMT_.getRunConfigurationsAndDataTakingCondition(runnumber)

  types_deleteCertificationData = []

  def export_deleteCertificationData(self):
    """It destroy the data used by the integration test"""
    return dataMGMT_.deleteCertificationData()

  #############################################################################
  types_getAvailableTagsFromSteps = []

  def export_getAvailableTagsFromSteps(self):
    """
    It returns the all used datatbase tags: DDDB, CondDB, DQTag
    """
    return dataMGMT_.getAvailableTagsFromSteps()
