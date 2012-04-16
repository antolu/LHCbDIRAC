########################################################################
# $Id$
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager

from types                                                                        import *
from DIRAC.Core.DISET.RequestHandler                                              import RequestHandler
from DIRAC                                                                        import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC.DataManagementSystem.Client.ReplicaManager                             import ReplicaManager
from DIRAC.Core.Utilities.Shifter                                                 import setupShifterProxyInEnv
from DIRAC.Core.Utilities                                                         import DEncode
import time,sys,os
import cPickle


dataMGMT_ = None

reader_ = None

global default
default = 'ALL'

def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
  global dataMGMT_
  dataMGMT_ = BookkeepingDatabaseClient()

  global reader_
  reader_ = XMLFilesReaderManager()

  return S_OK()

ToDoPath = gConfig.getValue( "stuart", "/opt/bookkeeping/XMLProcessing/ToDo" )

class BookkeepingManagerHandler( RequestHandler ):

  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [StringType]
  def export_echo(self, input):
    """ Echo input to output
    """
    return S_OK(input)

  #############################################################################
  types_sendBookkeeping = [StringType, StringType]
  def export_sendBookkeeping(self, name, xml):
    return self.export_sendXMLBookkeepingReport(xml)


  #############################################################################
  types_sendXMLBookkeepingReport = [StringType]
  def export_sendXMLBookkeepingReport(self, xml):
    result = S_ERROR()
    try:
      retVal = reader_.readXMLfromString(xml)
      if not retVal['OK']:
        result = S_ERROR(retVal['Message'])
      elif retVal['Value'] == '':
        result = S_OK("The send bookkeeping finished successfully!")
      else:
        result = retVal
    except Exception, x:
      errorMsg = 'The following error occurred during XML processing: %s ' % str(x)
      gLogger.error(errorMsg)
      result = errorMsg
    return result

  #############################################################################
  types_getAvailableSteps = [DictType]
  def export_getAvailableSteps(self, dict={}):
    return dataMGMT_.getAvailableSteps(dict)

  #############################################################################
  types_getRuntimeProjects = [DictType]
  def export_getRuntimeProjects(self, dict={}):
    return dataMGMT_.getRuntimeProjects(dict)

  #############################################################################
  types_getStepInputFiles = [IntType]
  def export_getStepInputFiles(self, StepId):
    result = S_ERROR()
    retVal = dataMGMT_.getStepInputFiles(StepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType', 'Visible']
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_setStepInputFiles = [IntType, ListType]
  def export_setStepInputFiles(self, stepid, files):
    return dataMGMT_.setStepInputFiles(stepid, files)

  #############################################################################
  types_setStepOutputFiles = [IntType, ListType]
  def export_setStepOutputFiles(self, stepid, files):
    return dataMGMT_.setStepOutputFiles(stepid, files)

  #############################################################################
  types_getStepOutputFiles = [IntType]
  def export_getStepOutputFiles(self, StepId):
    result = S_ERROR()
    retVal = dataMGMT_.getStepOutputFiles(StepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType', 'Visible']
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableFileTypes = []
  def export_getAvailableFileTypes(self):
    return dataMGMT_.getAvailableFileTypes()

  #############################################################################
  types_insertFileTypes = [StringType, StringType, StringType]
  def export_insertFileTypes(self, ftype, desc, fileType):
    return dataMGMT_.insertFileTypes(ftype, desc, fileType)

  #############################################################################
  types_insertStep = [DictType]
  def export_insertStep(self, dict):
    return dataMGMT_.insertStep(dict)

  #############################################################################
  types_deleteStep = [IntType]
  def export_deleteStep(self, stepid):
    return dataMGMT_.deleteStep(stepid)

  #############################################################################
  types_updateStep = [DictType]
  def export_updateStep(self, dict):
    return dataMGMT_.updateStep(dict)

  ##############################################################################
  types_getAvailableConfigNames = []
  def export_getAvailableConfigNames(self):
    retVal = dataMGMT_.getAvailableConfigNames()
    if retVal['OK']:
      records = []
      parameters = ['Configuration Name']
      for record in retVal['Value']:
        records += [list(record)]
      return S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      return retVal

  #############################################################################
  types_getConfigVersions = [DictType]
  def export_getConfigVersions(self, dict):
    result = S_ERROR()
    configName = dict.get('ConfigName', default)
    retVal = dataMGMT_.getConfigVersions(configName)
    if retVal['OK']:
      records = []
      parameters = ['Configuration Version']
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getConditions = [DictType]
  def export_getConditions(self, dict):
    result = S_ERROR()
    ok = True
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    evt = dict.get('EventType', dict.get('EventTypeId', default))

    if 'EventTypeId' in dict:
      gLogger.verbose('EventTypeId will be not accepted! Please change it to EventType')

    retVal = dataMGMT_.getConditions(configName, configVersion, evt)
    if retVal['OK']:
      values = retVal['Value']
      sim_parameters = ['SimId', 'Description', 'BeamCondition', 'BeamEnergy', 'Generator', 'MagneticField', 'DetectorCondition', 'Luminosity', 'G4settings' ]
      daq_parameters = ['DaqperiodId', 'Description', 'BeamCondition', 'BeanEnergy', 'MagneticField', 'VELO', 'IT', 'TT', 'OT', 'RICH1',
                          'RICH2', 'SPD_PRS', 'ECAL', 'HCAL', 'MUON', 'L0', 'HLT', 'VeloPosition']
      sim_records = []
      daq_records = []

      if len(values) > 0:
        for record in values:
          if record[0] != None:
            sim_records += [[record[0], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9]]]
          elif record[1] != None:
            daq_records += [[record[1], record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18], record[19], record[20], record[21], record[22], record[23], record[24], record[25], record[26]]]
          else:
            result = S_ERROR("Condition does not existis!")
            ok = False
      if ok:
        result = S_OK([{'ParameterNames':sim_parameters, 'Records':sim_records, 'TotalRecords':len(sim_records)}, {'ParameterNames':daq_parameters, 'Records':daq_records, 'TotalRecords':len(daq_records)}])
    else:
      result = retVal

    return result

  #############################################################################
  types_getProcessingPass = [DictType, StringType]
  def export_getProcessingPass(self, dict, path):
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    conddescription = dict.get('ConditionDescription', default)
    prod = dict.get('Production', default)
    runnb = dict.get('RunNumber', default)
    evt = dict.get('EventType', default)
    return dataMGMT_.getProcessingPass(configName, configVersion, conddescription, runnb, prod, evt, path)

  ############################################################################
  types_getStandardProcessingPass = [DictType, StringType]
  def export_getStandardProcessingPass(self, dict, path):
    result = S_ERROR()
    retVal = self.export_getProcessingPass(dict, path)
    if retVal['OK']:
      result = S_OK(retVal['Value'][0])
    else:
      result = retVal
    return result

  #############################################################################
  types_getProductions = [DictType]
  def export_getProductions(self, dict):
    result = S_ERROR()
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    conddescription = dict.get('ConditionDescription', default)
    processing = dict.get('ProcessingPass', default)
    evt = dict.get('EventType', dict.get('EventTypeId', default))

    if 'EventTypeId' in dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    retVal = dataMGMT_.getProductions(configName, configVersion, conddescription, processing, evt)
    if retVal['OK']:
      records = []
      parameters = ['Production/RunNumber']
      for record in retVal['Value']:
        records += [[record[0]]]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getFileTypes = [DictType]
  def export_getFileTypes(self, dict):
    result = S_ERROR()
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    conddescription = dict.get('ConditionDescription', default)
    processing = dict.get('ProcessingPass', default)
    evt = dict.get('EventType', dict.get('EventTypeId', default))
    production = dict.get('Production', default)
    runnb = dict.get('RunNumber', default)

    if 'EventTypeId' in dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    retVal = dataMGMT_.getFileTypes(configName, configVersion, conddescription, processing, evt, runnb, production)
    if retVal['OK']:
      records = []
      parameters = ['FileTypes']
      for record in retVal['Value']:
        records += [[record[0]]]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getStandardEventTypes = [DictType]
  def export_getStandardEventTypes(self, dict):
    self.export_getEventTypes(dict)

  #############################################################################
  def transfer_toClient(self, parameters, token, fileHelper):
    result = S_OK()
    dict = cPickle.loads(parameters)
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    conddescription = dict.get('ConditionDescription', default)
    processing = dict.get('ProcessingPass', default)
    evt = dict.get('EventType', dict.get('EventTypeId', default))
    production = dict.get('Production', default)
    filetype = dict.get('FileType', default)
    quality = dict.get('DataQuality', dict.get('Quality', default))
    runnb = dict.get('RunNumber', default)

    if 'EventTypeId' in dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    retVal = dataMGMT_.getFilesWithMetadata(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb)
    if retVal['OK']:
      records = []
      parameters = ['FileName', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd', 'WorkerNode', 'Name', 'RunNumber', 'FillNumber', 'FullStat', 'DataqualityFlag',
    'EventInputStat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity', 'TCK']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15], record[16]]]
      retVal = {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)}
      fileString = cPickle.dumps(retVal, protocol=2)
      retVal = fileHelper.stringToNetwork(fileString)
      if retVal['OK']:
        gLogger.info('Sent file %s of size %d' % (str(dict), len(fileString)))
      else:
        result = retVal
    else:
      result = retVal
    return result

  #############################################################################
  types_getFilesSumary = [DictType]
  def export_getFilesSumary(self, dict):
    return self.export_getFilesSummary(dict)

  #############################################################################
  types_getFilesSummary = [DictType]
  def export_getFilesSummary(self, dict):

    result = S_ERROR()
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    conddescription = dict.get('ConditionDescription', default)
    processing = dict.get('ProcessingPass', default)
    evt = dict.get('EventType', dict.get('EventTypeId', default))
    production = dict.get('Production', default)
    filetype = dict.get('FileType', default)
    quality = dict.get('DataQuality', dict.get('Quality', default))
    runnb = dict.get('RunNumbers', dict.get('RunNumber', default))
    startrun = dict.get('StartRun', default)
    endrun = dict.get('EndRun', default)

    if 'EventTypeId' in dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    retVal = dataMGMT_.getFilesSummary(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startrun, endrun)
    if retVal['OK']:
      records = []
      parameters = ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2], record[3], record[4]]]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getLimitedFiles = [DictType]
  def export_getLimitedFiles(self, dict):

    result = S_ERROR()
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    conddescription = dict.get('ConditionDescription', default)
    processing = dict.get('ProcessingPass', default)
    evt = dict.get('EventType', dict.get('EventTypeId', default))
    production = dict.get('Production', default)
    filetype = dict.get('FileType', default)
    quality = dict.get('DataQuality', dict.get('Quality', default))
    runnb = dict.get('RunNumbers', dict.get('RunNumber', default))
    start = dict.get('StartItem', 0)
    max = dict.get('MaxItem', 10)

    if 'EventTypeId' in dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    retVal = dataMGMT_.getLimitedFiles(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, start, max)
    if retVal['OK']:
      records = []
      parameters = ['Name', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd', 'WorkerNode', 'FileType', 'EventTypeId', 'RunNumber', 'FillNumber', 'FullStat', 'DataqualityFlag',
    'EventInputStat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity', 'TCK']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2], str(record[3]), str(record[4]), str(record[5]), record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17]]]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableDataQuality = []
  def export_getAvailableDataQuality(self):
    return dataMGMT_.getAvailableDataQuality()

  #############################################################################
  types_getAvailableProductions = []
  def export_getAvailableProductions(self):
    return dataMGMT_.getAvailableProductions()

  #############################################################################
  types_getAvailableRuns = []
  def export_getAvailableRuns(self):
    return dataMGMT_.getAvailableRuns()

  #############################################################################
  types_getAvailableEventTypes = []
  def export_getAvailableEventTypes(self):
    return dataMGMT_.getAvailableEventTypes()

  #############################################################################
  types_getMoreProductionInformations = [IntType]
  def export_getMoreProductionInformations(self, prodid):
    return dataMGMT_.getMoreProductionInformations(prodid)

  #############################################################################
  types_getJobInfo = [StringType]
  def export_getJobInfo(self, lfn):
    return dataMGMT_.getJobInfo(lfn)

  #############################################################################
  types_getRunNumber = [StringType]
  def export_getRunNumber(self, lfn):
    return dataMGMT_.getRunNumber(lfn)

  #############################################################################
  types_getRunNbAndTck = [StringType]
  def export_getRunNbAndTck(self, lfn):
    return dataMGMT_.getRunNbAndTck(lfn)

  #############################################################################
  types_getProductionFiles = [IntType, StringType]
  def export_getProductionFiles(self, prod, fileType, replica=default):
    return dataMGMT_.getProductionFiles(int(prod), fileType, replica)

  #############################################################################
  types_getAvailableRunNumbers = []
  def export_getAvailableRunNumbers(self):
    return self.export_getAvailableRuns()

  #############################################################################
  types_getRunFiles = [IntType]
  def export_getRunFiles(self, runid):
    return dataMGMT_.getRunFiles(runid)

  #############################################################################
  types_updateFileMetaData = [StringType, DictType]
  def export_updateFileMetaData(self, filename, fileAttr):
    return dataMGMT_.updateFileMetaData(filename, fileAttr)

  #############################################################################
  types_renameFile = [StringType, StringType]
  def export_renameFile(self, oldLFN, newLFN):
    return dataMGMT_.renameFile(oldLFN, newLFN)

  #############################################################################
  types_getInputAndOutputJobFiles = [ListType]
  def export_getInputAndOutputJobFiles(self, jobids):
    return self.export_getJobInputAndOutputJobFiles(jobids)

  #############################################################################
  types_getJobInputAndOutputJobFiles = [ListType]
  def export_getJobInputAndOutputJobFiles(self, jobids):
    return dataMGMT_.getJobInputAndOutputJobFiles(jobids)

  #############################################################################
  types_getProductionProcessingPassID = [LongType]
  def export_getProductionProcessingPassID(self, prodid):
    return dataMGMT_.getProductionProcessingPassID(prodid)

  #############################################################################
  types_getProductionProcessingPass = [LongType]
  def export_getProductionProcessingPass(self, prodid):
    return dataMGMT_.getProductionProcessingPass(prodid)

  #############################################################################
  types_insertTag = [DictType]
  def export_insertTag(self, values):
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
    return S_OK({'Successfull':successfull, 'Faild':faild})

  #############################################################################
  types_setQuality = [ListType, StringType]
  def export_setQuality(self, lfns, flag):
    return self.export_setFileDataQuality(lfns, flag)

  #############################################################################
  types_setFileDataQuality = [ListType, StringType]
  def export_setFileDataQuality(self, lfns, flag):
    return dataMGMT_.setFileQuality(lfns, flag)

  #############################################################################
  types_setRunAndProcessingPassDataQuality = [LongType, StringType, StringType]
  def export_setRunAndProcessingPassDataQuality(self, runNB, procpass, flag):
    return dataMGMT_.setRunAndProcessingPassDataQuality(runNB, procpass, flag)

  #############################################################################
  types_setRunQualityWithProcessing = [LongType, StringType, StringType]
  def export_setRunQualityWithProcessing(self, runNB, procpass, flag):
    return self.export_setRunAndProcessingPassDataQuality(runNB, procpass, flag)

  #############################################################################
  types_setRunDataQuality = [IntType, StringType]
  def export_setRunDataQuality(self, runNb, flag):
    return dataMGMT_.setRunDataQuality(runNb, flag)

  #############################################################################
  types_setQualityRun = [IntType, StringType]
  def export_setQualityRun(self, runNb, flag):
    return dataMGMT_.setRunDataQuality(runNb, flag)

  #############################################################################
  types_setProductionDataQuality = [IntType, StringType]
  def export_setProductionDataQuality(self, prod, flag):
    return dataMGMT_.setProductionDataQuality(prod, flag)

  #############################################################################
  types_setQualityProduction = [IntType, StringType]
  def export_setQualityProduction(self, prod, flag):
    return self.export_setProductionDataQuality(prod, flag)

  types_getLFNsByProduction = [IntType]
  def export_getLFNsByProduction(self, prod):
    return self.export_getProductionFiles(prod, 'ALL', 'ALL')

  #############################################################################
  types_getFileAncestors = [ListType, IntType, BooleanType]
  def export_getFileAncestors(self, lfns, depth, replica):
    return dataMGMT_.getFileAncestors(lfns, depth, replica)

  #############################################################################
  types_getAllAncestors = [ListType, IntType]
  def export_getAllAncestors(self, lfns, depth):
    result = S_ERROR()
    retVal = dataMGMT_.getFileAncestors(lfns, depth, False)
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      result = S_OK(values)
    else:
      result = retVal
    return result

  #############################################################################
  types_getAncestors = [ListType, IntType]
  def export_getAncestors(self, lfns, depth):
    result = S_ERROR()
    retVal = dataMGMT_.getFileAncestors(lfns, depth, True)
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      result = S_OK(values)
    else:
      result = retVal
    return result

  #############################################################################
  types_getAllAncestorsWithFileMetaData = [ListType, IntType]
  def export_getAllAncestorsWithFileMetaData(self, lfns, depth):
    return dataMGMT_.getFileAncestors(lfns, depth, False)

  #############################################################################
  types_getAllDescendents = [ListType, IntType, IntType, BooleanType]
  def export_getAllDescendents(self, lfn, depth=0, production=0, checkreplica=False):
    return dataMGMT_.getFileDescendents(lfn, depth, production, checkreplica)

  #############################################################################
  types_getDescendents = [ListType, IntType]
  def export_getDescendents(self, lfn, depth):
    return self.export_getFileDescendents(lfn, depth)

  #############################################################################
  types_getFileDescendents = [ListType, IntType, IntType, BooleanType]
  def export_getFileDescendents(self, lfn, depth, production=0, checkreplica=True):
    return dataMGMT_.getFileDescendents(lfn, depth, production, checkreplica)

  #############################################################################
  types_checkfile = [StringType]
  def export_checkfile(self, fileName):
    return dataMGMT_.checkfile(fileName)

  #############################################################################
  types_checkFileTypeAndVersion = [StringType, StringType]
  def export_checkFileTypeAndVersion(self, type, version):
    return dataMGMT_.checkFileTypeAndVersion(type, version)

  #############################################################################
  types_checkEventType = [LongType]
  def export_checkEventType(self, eventTypeId):
    return dataMGMT_.checkEventType(eventTypeId)

  #############################################################################
  types_insertSimConditions = [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings):
    return dataMGMT_.insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings)

  #############################################################################
  types_getSimConditions = []
  def export_getSimConditions(self):
    return dataMGMT_.getSimConditions()

  #############################################################################
  types_removeReplica = [StringType]
  def export_removeReplica(self, fileName):
    return dataMGMT_.removeReplica(fileName)

  #############################################################################
  types_getFileMetadata = [ListType]
  def export_getFileMetadata(self, lfns):
    return dataMGMT_.getFileMetadata(lfns)

  #############################################################################
  types_getFilesInformations = [ListType]
  def export_getFilesInformations(self, lfns):
    return dataMGMT_.getFileMetadata(lfns)

  #############################################################################
  types_getFileMetaDataForUsers = [ListType]
  def export_getFileMetaDataForUsers(self, lfns):
    res = dataMGMT_.getFileMetaDataForWeb(lfns)
    return res

  #############################################################################
  types_getFileMetaDataForWeb = [ListType]
  def export_getFileMetaDataForWeb(self, lfns):
    res = dataMGMT_.getFileMetaDataForWeb(lfns)
    return res

  #############################################################################
  types_getProductionFilesForUsers = [IntType, DictType, DictType, LongType, LongType]
  def export_getProductionFilesForUsers(self, prod, ftype, SortDict, StartItem, Maxitems):
    res = dataMGMT_.getProductionFilesForWeb(prod, ftype, SortDict, StartItem, Maxitems)
    return res

  #############################################################################
  types_getProductionFilesForWeb = [IntType, DictType, DictType, LongType, LongType]
  def export_getProductionFilesWeb(self, prod, ftype, SortDict, StartItem, Maxitems):
    res = dataMGMT_.getProductionFilesForWeb(prod, ftype, SortDict, StartItem, Maxitems)
    return res

  #############################################################################
  types_exists = [ListType]
  def export_exists(self, lfns):
    return dataMGMT_.exists(lfns)

  #############################################################################
  types_addReplica = [StringType]
  def export_addReplica(self, fileName):
    return dataMGMT_.addReplica(fileName)

  #############################################################################
  types_getRunInformations = [IntType]
  def export_getRunInformations(self, runnb):
    return dataMGMT_.getRunInformations(runnb)

  #############################################################################
  types_getRunInformation = [DictType]
  def export_getRunInformation(self, runnb):
    return dataMGMT_.getRunInformation(runnb)

  #############################################################################
  types_getFileCreationLog = [StringType]
  def export_getFileCreationLog(self, lfn):
    return dataMGMT_.getFileCreationLog(lfn)

  #############################################################################
  types_getLogfile = [StringType]
  def export_getLogfile(self, lfn):
    return dataMGMT_.getFileCreationLog(lfn)

  #############################################################################
  types_insertEventType = [LongType, StringType, StringType]
  def export_insertEventType(self, evid, desc, primary):
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
  types_addEventType = [LongType, StringType, StringType]
  def export_addEventType(self, evid, desc, primary):
    return self.export_insertEventType(evid, desc, primary)

  #############################################################################
  types_updateEventType = [LongType, StringType, StringType]
  def export_updateEventType(self, evid, desc, primary):
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
  types_addFiles = [ListType]
  def export_addFiles(self, lfns):
    result = {}
    for file in lfns:
      res = dataMGMT_.addReplica(file)
      if not res['OK']:
        result[file] = res['Message']
    return S_OK(result)

  #############################################################################
  types_removeFiles = [ListType]
  def export_removeFiles(self, lfns):
    result = {}
    for file in lfns:
      res = dataMGMT_.removeReplica(file)
      if not res['OK']:
        result[file] = res['Message']
    return S_OK(result)

  #############################################################################
  types_getProductionSummary = [DictType]
  def export_getProductionSummary(self, dict):

    cName = dict.get('ConfigName', default)
    cVersion = dict.get('ConfigVersion', default)
    production = dict.get('Production', default)
    simdesc = dict.get('ConditionDescription', default)
    pgroup = dict.get('ProcessingPass', default)
    ftype = dict.get('FileType', default)
    evttype = dict.get('EventType', default)
    retVal = dataMGMT_.getProductionSummary(cName, cVersion, simdesc, pgroup, production, ftype, evttype)

    return retVal

  #############################################################################
  types_getProductionInformations_new = [LongType]
  def export_getProductionInformations_new(self, prodid):
    return self.export_getProductionInformations(prodid)

  #############################################################################
  types_getProductionInformations = [LongType]
  def export_getProductionInformations(self, prodid):

    nbjobs = None
    nbOfFiles = None
    sizeofFiles = None
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
      result = {"Production informations":prodinfos, "Steps":steps, "Number of jobs":nbjobs, "Number of files":nbOfFiles, "Number of events":nbOfEvents, 'Path':path}
      return S_OK(result)

      #return S_ERROR(value['Message'])


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
    result = {"Production informations":prodinfos, "Steps":steps, "Number of jobs":nbjobs, "Number of files":nbOfFiles, "Number of events":nbOfEvents, 'Path':path}
    return S_OK(result)

  #############################################################################
  types_getProductionInformationsFromView = [LongType]
  def export_getProductionInformationsFromView(self, prodid):
    result = S_ERROR()
    value = dataMGMT_.getProductionInformationsFromView(prodid)
    parameters = []
    infos = []
    if value['OK']:
      records = value['Value']
      parameters = ['Production', 'EventTypeId', 'FileType', 'NumberOfEvents', 'NumberOfFiles']
      for record in records:
        infos += [[record[0], record[1], record[2], record[3], record[4]]]
      result = S_OK({'ParameterNames':parameters, 'Records':infos})
    else:
      result = value
    return result

  #############################################################################
  types_getFileHistory = [StringType]
  def export_getFileHistory(self, lfn):
    retVal = dataMGMT_.getFileHistory(lfn)
    result = {}
    records = []
    if retVal['OK']:
      values = retVal['Value']
      parameterNames = ['FileId', 'FileName', 'ADLER32', 'CreationDate', 'EventStat', 'EventtypeId', 'Gotreplica', 'GUI', 'JobId', 'md5sum', 'FileSize', 'FullStat', 'Dataquality', 'FileInsertDate', 'Luminosity', 'InstLuminosity']
      sum = 0
      for record in values:
        value = [record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15]]
        records += [value]
        sum += 1
      result = {'ParameterNames':parameterNames, 'Records':records, 'TotalRecords':sum}
    else:
      result = S_ERROR(retVal['Message'])
    return S_OK(result)

  #############################################################################
  types_getJobsNb = [LongType]
  def export_getJobsNb(self, prodid):
    return dataMGMT_.getProductionNbOfJobs(prodid)

  #############################################################################
  types_getProductionNbOfJobs = [LongType]
  def export_getProductionNbOfJobs(self, prodid):
    return dataMGMT_.getProductionNbOfJobs(prodid)

  #############################################################################
  types_getNumberOfEvents = [LongType]
  def export_getNumberOfEvents(self, prodid):
    return dataMGMT_.getProductionNbOfEvents(prodid)

  #############################################################################
  types_getProductionNbOfEvents = [LongType]
  def export_getProductionNbOfEvents(self, prodid):
    return dataMGMT_.getProductionNbOfEvents(prodid)

  #############################################################################
  types_getSizeOfFiles = [LongType]
  def export_getSizeOfFiles(self, prodid):
    return dataMGMT_.getProductionSizeOfFiles(prodid)

  #############################################################################
  types_getProductionSizeOfFiles = [LongType]
  def export_getProductionSizeOfFiles(self, prodid):
    return dataMGMT_.getProductionSizeOfFiles(prodid)


  #############################################################################
  types_getNbOfFiles = [LongType]
  def export_getNbOfFiles(self, prodid):
    return dataMGMT_.getProductionNbOfFiles(prodid)

  #############################################################################
  types_getProductionNbOfFiles = [LongType]
  def export_getProductionNbOfFiles(self, prodid):
    return dataMGMT_.getProductionNbOfFiles(prodid)

  #############################################################################
  types_getProductionInformation = [LongType]
  def export_getProductionInformation(self, prodid):
    return dataMGMT_.getProductionInformation(prodid)

  #############################################################################
  types_getNbOfJobsBySites = [LongType]
  def export_getNbOfJobsBySites(self, prodid):
    return dataMGMT_.getNbOfJobsBySites(prodid)

  #############################################################################
  types_getAvailableTags = []
  def export_getAvailableTags(self):
    return dataMGMT_.getAvailableTags()

  #############################################################################
  types_getProcessedEvents = [IntType]
  def export_getProcessedEvents(self, prodid):
    return dataMGMT_.getProductionProcessedEvents(prodid)

  #############################################################################
  types_getProductionProcessedEvents = [IntType]
  def export_getProductionProcessedEvents(self, prodid):
    return dataMGMT_.getProductionProcessedEvents(prodid)

  #############################################################################
  types_getRunsWithAGivenDates = [DictType]
  def export_getRunsWithAGivenDates(self, dict):
    return self.export_getRunsForAGivenPeriod(dict)

  #############################################################################
  types_getRunsForAGivenPeriod = [DictType]
  def export_getRunsForAGivenPeriod(self, dict):
    return dataMGMT_.getRunsForAGivenPeriod(dict)

  #############################################################################
  types_getProductiosWithAGivenRunAndProcessing = [DictType]
  def export_getProductiosWithAGivenRunAndProcessing(self, dict):
    return self.export_getProductionsFromView(dict)

  #############################################################################
  types_getProductionsFromView = [DictType]
  def export_getProductionsFromView(self, dict):
    return dataMGMT_.getProductionsFromView(dict)

  #############################################################################
  types_getDataQualityForRuns = [ListType]
  def export_getDataQualityForRuns(self, runs):
    return self.export_getRunFilesDataQuality(runs)

  #############################################################################
  types_getRunFilesDataQuality = [ListType]
  def export_getRunFilesDataQuality(self, runs):
    return dataMGMT_.getRunFilesDataQuality(runs)

  #############################################################################
  types_setFilesInvisible = [ListType]
  def export_setFilesInvisible(self, lfns):
    return dataMGMT_.setFilesInvisible(lfns)

  #############################################################################
  types_setFilesVisible = [ListType]
  def export_setFilesVisible(self, lfns):
    return dataMGMT_.setFilesVisible(lfns)

  #############################################################################
  types_getRunFlag = [LongType, LongType]
  def export_getRunFlag(self, runnb, processing):
    return self.export_getRunAndProcessingPassDataQuality(runnb, processing)

  #############################################################################
  types_getRunAndProcessingPassDataQuality = [LongType, LongType]
  def export_getRunAndProcessingPassDataQuality(self, runnb, processing):
    return dataMGMT_.getRunAndProcessingPassDataQuality(runnb, processing)

  #############################################################################
  types_getAvailableConfigurations = []
  def export_getAvailableConfigurations(self):
    return dataMGMT_.getAvailableConfigurations()

  #############################################################################
  types_getRunProcessingPass = [LongType]
  def export_getRunProcessingPass(self, runnumber):
    return dataMGMT_.getRunProcessingPass(runnumber)

  #############################################################################
  types_getProductionFilesStatus = [IntType, ListType]
  def export_getProductionFilesStatus(self, productionid=None, lfns = []):
    return dataMGMT_.getProductionFilesStatus(productionid, lfns)

  #############################################################################
  types_getFilesWithGivenDataSets = [DictType]
  def export_getFilesWithGivenDataSets(self, values):
    gLogger.debug('getFiles dataset:'+str(values))
    return self.export_getFiles(values)

  #############################################################################
  types_getFiles = [DictType]
  def export_getFiles(self, values):

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
    replicaFlag = values.get('ReplicaFlag', 'Yes')
    visible = values.get('Visible', default)
    filesize = values.get('FileSize', False)
    tck = values.get('TCK', [])

    if 'ProductionID' in values:
      gLogger.verbose('ProductionID will be removed. It will changed to Production')

    if 'DataQualityFlag' in values:
      gLogger.verbose('DataQualityFlag will be removed. It will changed to DataQuality')

    if 'RunNumbers' in values:
      gLogger.verbose('RunNumbers will be removed. It will changed to RunNumbers')

    result = []
    retVal = dataMGMT_.getFiles(simdesc, datataking, procPass, ftype, evt, configname, configversion, prod, flag, startd, endd, nbofevents, startRunID, endRunID, runNbs, replicaFlag, visible, filesize, tck)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      values = retVal['Value']
      for i in values:
        result += [i[0]]

    return S_OK(result)

  #############################################################################
  types_getFilesWithGivenDataSetsForUsers = [DictType]
  def export_getFilesWithGivenDataSetsForUsers(self, values):
    return self.export_getVisibleFilesWithMetadata(values)

  #############################################################################
  types_getVisibleFilesWithMetadata = [DictType]
  def export_getVisibleFilesWithMetadata(self, values):

    simdesc = values.get('SimulationConditions',default)
    datataking = values.get('DataTakingConditions',default)
    procPass = values.get('ProcessingPass', default)
    ftype = values.get('FileType', default)
    evt = values.get('EventType', 0)
    configname = values.get('ConfigName',default)
    configversion = values.get('ConfigVersion', default)
    prod = values.get('Production', values.get('ProductionID', default))
    flag = values.get('DataQuality', values.get('DataQualityFlag', default))
    startd = values.get('StartDate', None)
    endd = values.get('EndDate', None)
    nbofevents = values.get('NbOfEvents', False)
    startRunID = values.get('StartRun', None)
    endRunID = values.get('EndRun', None)
    runNbs = values.get('RunNumber', values.get('RunNumbers', []))
    replicaFlag = values.get('ReplicaFlag', 'Yes')
    tck = values.get('TCK', [])

    if ftype == default:
      return S_ERROR('FileType is missing!')

    if 'ProductionID' in values:
      gLogger.verbose('ProductionID will be removed. It will changed to Production')

    if 'DataQualityFlag' in values:
      gLogger.verbose('DataQualityFlag will be removed. It will changed to DataQuality')

    if 'RunNumbers' in values:
      gLogger.verbose('RunNumbers will be removed. It will changed to RunNumbers')

    result = {}
    retVal = dataMGMT_.getVisibleFilesWithMetadata(simdesc, datataking, procPass, ftype, evt, configname, configversion, prod, flag, startd, endd, nbofevents, startRunID, endRunID, runNbs, replicaFlag, tck)
    summary = 0
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
        if i[1] != None:
          nbevents += i[1]
        if i[2] != None:
          evinput += i[2]
        if i[5] != None:
          fsize += i[5]
        if i[6] != None:
          tLumi += i[6]
        if i[7] != None:
          lumi += i[7]
        if i[8] != None:
          ilumi += i[8]
        result[i[0]] = {'EventStat':i[1], 'EventInputStat':i[2], 'Runnumber':i[3], 'Fillnumber':i[4], 'FileSize':i[5], 'TotalLuminosity':i[6], 'Luminosity':i[7], 'InstLuminosity':i[8], 'TCK':i[9]}
      if nbfiles > 0:
        summary = {'Number Of Files':nbfiles, 'Number of Events':nbevents, 'EventInputStat':evinput, 'FileSize':fsize / 1000000000., 'TotalLuminosity':tLumi, 'Luminosity':lumi, 'InstLuminosity':ilumi}
    return S_OK({'LFNs' : result, 'Summary': summary})

  #############################################################################
  types_addProduction = [DictType]
  def export_addProduction(self, infos):
    gLogger.debug(infos)
    result = None
    simcond = infos.get('SimulationConditions', None)
    daqdesc = infos.get('DataTakingConditions', None)
    production = None

    if (simcond == None and daqdesc == None):
      result = S_ERROR('SimulationConditions or DataTakingConditins is missing!')

    if 'Steps' not in infos:
      result = S_ERROR("Missing Steps!")
    if 'Production' not in infos:
      result = S_ERROR('Production is missing!')

    if not result:
      steps = infos['Steps']
      inputProdTotalProcessingPass = ''
      production = infos['Production']
      inputProdTotalProcessingPass = infos.get('InputProductionTotalProcessingPass', '')
      result = dataMGMT_.addProduction(production, simcond, daqdesc, steps, inputProdTotalProcessingPass)
    return result

  #############################################################################
  types_getEventTypes = [DictType]
  def export_getEventTypes(self, dict):
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    production = dict.get('Production', default)
    return  dataMGMT_.getEventTypes(configName, configVersion, production)

  #############################################################################
  types_getProcessingPassSteps = [DictType]
  def export_getProcessingPassSteps(self, dict):
    stepname = dict.get('StepName', default)
    cond = dict.get('ConditionDescription', default)
    procpass = dict.get('ProcessingPass', default)

    return dataMGMT_.getProcessingPassSteps(procpass, cond, stepname)

  #############################################################################
  types_getProductionProcessingPassSteps = [DictType]
  def export_getProductionProcessingPassSteps(self, dict):
    if 'Production' in dict:
      return dataMGMT_.getProductionProcessingPassSteps(dict['Production'])
    else:
      return S_ERROR('The Production dictionary key is missing!!!')

  #############################################################################
  types_getProductionOutputFiles = [DictType]
  def export_getProductionOutputFiles(self, dict):
    return self.export_getProductionOutputFileTypes(dict)

  #############################################################################
  types_getProductionOutputFileTypes = [DictType]
  def export_getProductionOutputFileTypes(self, dict):
    production = dict.get('Production', default)
    if production != default:
      return dataMGMT_.getProductionOutputFileTypes(production)
    else:
      return S_ERROR('The Production dictionary key is missing!!!')

  #############################################################################
  types_getRunQuality = [StringType, StringType]
  def export_getRunQuality(self, procpass, flag=default):
    return self.export_getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  types_getRunWithProcessingPassAndDataQuality = [StringType, StringType]
  def export_getRunWithProcessingPassAndDataQuality(self, procpass, flag=default):
    return dataMGMT_.getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  types_getRuns = [DictType]
  def export_getRuns(self, dict):
    result = S_ERROR()
    cName = dict.get('ConfigName', default)
    cVersion = dict.get('ConfigVersion', default)
    if cName != default and cVersion != default:
      result = dataMGMT_.getRuns(cName, cVersion)
    else:
      result = S_ERROR('The configuration name and version have to be defined!')
    return result

  #############################################################################
  types_getRunProcPass = [DictType]
  def export_getRunProcPass(self, dict):
    return self.export_getRunAndProcessingPass(dict)

  #############################################################################
  types_getRunAndProcessingPass = [DictType]
  def export_getRunAndProcessingPass(self, dict):
    run = dict.get('RunNumber', default)
    result = S_ERROR()
    if run != default:
      result = dataMGMT_.getRunAndProcessingPass(run)
    else:
      result = S_ERROR('The run number has to be specified!')
    return result

  #############################################################################
  types_getProcessingPassId = [StringType]
  def export_getProcessingPassId(self, fullpath):
    return dataMGMT_.getProcessingPassId(fullpath)

  #############################################################################
  types_getRunNbFiles = [DictType]
  def export_getRunNbFiles(self, dict):
    return self.export_getNbOfRawFiles(dict)

  #############################################################################
  types_getNbOfRawFiles = [DictType]
  def export_getNbOfRawFiles(self, dict):

    runnb = dict.get('RunNumber', default)
    evt = dict.get('EventTypeId', default)
    result = S_ERROR()
    if runnb == default and evt == default:
      result = S_ERROR('Run number or event type must be given!')
    else:
      retVal = dataMGMT_.getNbOfRawFiles(runnb, evt)
      if retVal['OK']:
        result = S_OK(retVal['Value'][0][0])
      else:
        result = retVal
    return result


  #############################################################################
  types_getTypeVersion = [ListType]
  def export_getTypeVersion(self, lfn):
    return self.export_getFileTypeVersion(lfn)

  #############################################################################
  types_getFileTypeVersion = [ListType]
  def export_getFileTypeVersion(self, lfn):
    return dataMGMT_.getFileTypeVersion(lfn)

  #############################################################################
  types_getTCKs = [DictType]
  def export_getTCKs(self, dict):
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion',default)
    conddescription = dict.get('ConditionDescription',default)
    processing = dict.get('ProcessingPass',default)
    evt = dict.get('EventTypeId',default)
    production = dict.get('Production',default)
    filetype = dict.get('FileType',default)
    quality = dict.get('DataQuality',dict.get('Quality',default))
    runnb = dict.get('RunNumber',default)
    result = S_ERROR()
    if 'Quality' in  dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    retVal = dataMGMT_.getTCKs(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb)
    if retVal['OK']:
      records = []
      for record in retVal['Value']:
        records += [record[0]]
      result = S_OK(records)
    else:
      result =  retVal
    return result

  #############################################################################
  types_getAvailableTcks = [DictType]
  def export_getAvailableTcks(self, dict):
    return self.export_getTCKs(dict)

  #############################################################################
  types_getStepsMetadata = [DictType]
  def export_getStepsMetadata(self, dict):
    configName = dict.get('ConfigName', default)
    configVersion = dict.get('ConfigVersion', default)
    cond = dict.get('ConditionDescription', default)
    procpass = dict.get('ProcessingPass', default)
    evt = dict.get('EventType', dict.get('EventTypeId', default))
    production = dict.get('Production', default)
    filetype = dict.get('FileType', default)
    runnb = dict.get('RunNumber', default)

    if 'EventTypeId' in dict:
      gLogger.verbose('The EventTypeId has to be replaced by EventType!')

    if 'Quality' in dict:
      gLogger.verbose('The Quality has to be replaced by DataQuality!')

    return dataMGMT_.getStepsMetadata(configName, configVersion, cond, procpass, evt, production, filetype, runnb)

  #############################################################################
  types_getDirectoryMetadata = [StringType]
  def export_getDirectoryMetadata(self, lfn):
    return dataMGMT_.getDirectoryMetadata(lfn)

  #############################################################################
  types_getFilesForGUID = [StringType]
  def export_getFilesForGUID(self, guid):
    return dataMGMT_.getFilesForGUID(guid)
