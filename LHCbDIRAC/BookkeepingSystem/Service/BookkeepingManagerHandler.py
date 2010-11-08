########################################################################
# $Id$
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager

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

def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
  global dataMGMT_
  dataMGMT_ = BookkeepingDatabaseClient()
  
  global reader_
  reader_ = XMLFilesReaderManager()

  return S_OK()

ToDoPath = gConfig.getValue("stuart","/opt/bookkeeping/XMLProcessing/ToDo")
    
class BookkeepingManagerHandler(RequestHandler):

  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed 
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [StringType]
  def export_echo(self,input):
    """ Echo input to output
    """
    return S_OK(input)

  #############################################################################
  types_sendBookkeeping = [StringType, StringType]
  def export_sendBookkeeping(self, name, data):
      """
      This method send XML file to the ToDo directory
      """
      try:
          result  = reader_.readXMLfromString(name, data)
          if not result['OK']:
            return S_ERROR(result['Message'])
          if result['Value']=='':
            return S_OK("The send bookkeeping finished successfully!")
          else:
            return result
          """
          stamp = time.strftime('%Y.%m.%d-%H.%M.%S',time.gmtime())
          
          fileID=int(repr(time.time()).split('.')[1])
          
          filePath ="%s%s.%08d.%s"%(ToDoPath+os.sep, stamp, fileID, name)  
          update_file = open(filePath, "w")
          
          print >>update_file, data
          update_file.close()
          #copyXML(filePath)
          """
          return #S_OK("The send bookkeeping finished successfully!")
      except Exception, x:
          print str(x)
          return S_ERROR('Error during processing '+name)
  
  #############################################################################
  types_getAvailableSteps = [DictType]
  def export_getAvailableSteps(self, dict = {}):
    retVal = dataMGMT_.getAvailableSteps(dict)
    if retVal['OK']:
      if dict.has_key('StepId'):
        parameters = ['FileType', 'Visible']
        records = []
        for record in retVal['Value']:
          value = [record[0], record[1]]
          records += [value]
      else:
        parameters = ['StepId', 'StepName','ApplicationName', 'ApplicationVersion','OptionFiles','DDDB','CONDDB','ExtraPackages','Visible']
        records = []
        for record in retVal['Value']:
          value = [record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8]]
          records += [value]
      parameters = ['StepId','StepName', 'ApplicationName','ApplicationVersion','OptionFiles','DDDB','CONDDB', 'ExtraPackages','Visible']
      records = []
      for record in retVal['Value']:
        records += [[record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  types_getStepInputFiles = [IntType]
  def export_getStepInputFiles(self, StepId):
    retVal = dataMGMT_.getStepInputFiles(StepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType','Visible']
      for record in retVal['Value']:
        records += [[record[0],record[1]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
  
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
    retVal = dataMGMT_.getStepOutputFiles(StepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType','Visible']
      for record in retVal['Value']:
        records += [[record[0],record[1]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
    
  #############################################################################
  types_getAvailableFileTypes = []
  def export_getAvailableFileTypes(self):
    return dataMGMT_.getAvailableFileTypes()
  
  #############################################################################
  types_insertFileTypes = [StringType,StringType]
  def export_insertFileTypes(self, ftype, desc):
    return dataMGMT_.insertFileTypes(ftype, desc)
  
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
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
    
  #############################################################################
  types_getConfigVersions = [DictType]
  def export_getConfigVersions(self, dict):
    
    configName = 'ALL'
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
      
    retVal =  dataMGMT_.getConfigVersions(configName)
    if retVal['OK']:
      records = []
      parameters = ['Configuration Version']
      for record in retVal['Value']:
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
  
 #############################################################################
  types_getConditions = [DictType]
  def export_getConditions(self, dict):
    configName = 'ALL'
    configVersion='ALL'
    
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
    
    if dict.has_key('ConfigVersion'):
      configVersion = dict['ConfigVersion']
    
    retVal =  dataMGMT_.getConditions(configName, configVersion)
    if retVal['OK']:
      values = retVal['Value']
      if len(values) > 0:
        if values[0][0] != None:
          records = []
          parameters = ['SimId','Description', 'BeamCondition', 'BeamEnergy', 'Generator', 'MagneticField', 'DetectorCondition', 'Luminosity' ]
          for record in values:
            records += [[record[0], record[2],record[3],record[4],record[5],record[6],record[7],record[8]]]
          return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
        elif values[0][1] != None:
          records = []
          parameters = ['DaqperiodId','Description', 'BeamCondition','BeanEnergy','MagneticField', 'VELO', 'IT', 'TT', 'OT','RICH1',
                        'RICH2', 'SPD_PRS', 'ECAL', 'HCAL', 'MUON', 'L0', 'HLT', 'VeloPosition']
          for record in values:
            records += [[record[1], record[9],record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18], record[19], record[20], record[21], record[22], record[23], record[24], record[25]]]
          return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
        else:
          return S_ERROR("Condition does not existis!")
      else:
        return S_ERROR("Condition does not existis!")
    else:
      return retVal
  
  #############################################################################
  types_getProcessingPass = [DictType, StringType]
  def export_getProcessingPass(self, dict, path):
    configName = 'ALL'
    configVersion='ALL'
    conddescription='ALL'
    
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
    
    if dict.has_key('ConfigVersion'):
      configVersion = dict['ConfigVersion']
    
    if dict.has_key('ConditionDescription'):
      conddescription = dict['ConditionDescription']
    
    return dataMGMT_.getProcessingPass(configName, configVersion, conddescription, path)
    
  #############################################################################
  types_getProductions = [DictType]
  def export_getProductions(self, dict):
    configName = 'ALL'
    configVersion='ALL' 
    conddescription='ALL' 
    processing='ALL'
    evt='ALL'
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
    
    if dict.has_key('ConfigVersion'):
      configVersion = dict['ConfigVersion']
    
    if dict.has_key('ConditionDescription'):
      conddescription = dict['ConditionDescription']
    
    if dict.has_key('ProcessingPass'):
      processing = dict['ProcessingPass']
    
    if dict.has_key('EventTypeId'):
      evt = dict['EventTypeId']
    
    retVal = dataMGMT_.getProductions(configName, configVersion, conddescription, processing, evt)
    if retVal['OK']:
      records = []
      parameters = ['Production/RunNumber']
      for record in retVal['Value']:
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal 
  
  #############################################################################
  types_getFileTypes = [DictType]
  def export_getFileTypes(self, dict):
    configName = 'ALL'
    configVersion='ALL' 
    conddescription='ALL' 
    processing='ALL'
    evt='ALL'
    production = 'ALL'
    
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
    
    if dict.has_key('ConfigVersion'):
      configVersion = dict['ConfigVersion']
    
    if dict.has_key('ConditionDescription'):
      conddescription = dict['ConditionDescription']
    
    if dict.has_key('ProcessingPass'):
      processing = dict['ProcessingPass']
    
    if dict.has_key('EventTypeId'):
      evt = dict['EventTypeId']
    
    if dict.has_key('Production'):
      production = dict['Production']
    
    retVal = dataMGMT_.getFileTypes(configName, configVersion, conddescription, processing, evt, production)
    if retVal['OK']:
      records = []
      parameters = ['FileTypes']
      for record in retVal['Value']:
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
    
  #############################################################################
  def transfer_toClient( self, parameters, token, fileHelper ):
    dict = cPickle.loads(parameters)
    configName = 'ALL'
    configVersion='ALL' 
    conddescription='ALL' 
    processing='ALL'
    evt='ALL'
    production = 'ALL'
    filetype = 'ALL'
    quality = 'ALL'
    
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
    
    if dict.has_key('ConfigVersion'):
      configVersion = dict['ConfigVersion']
    
    if dict.has_key('ConditionDescription'):
      conddescription = dict['ConditionDescription']
    
    if dict.has_key('ProcessingPass'):
      processing = dict['ProcessingPass']
    
    if dict.has_key('EventTypeId'):
      evt = dict['EventTypeId']
    
    if dict.has_key('Production'):
      production = dict['Production']
    
    if dict.has_key('FileType'):
      filetype = dict['FileType']
    
    if dict.has_key('Quality'):
      quality = dict['Quality']
      
    retVal = dataMGMT_.getFiles(configName, configVersion, conddescription, processing, evt, production, filetype, quality)  
    if retVal['OK']:
      records = []
      parameters = ['FileName', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd', 'WorkerNode', 'Name', 'RunNumber', 'FillNumber', 'FullStat', 'DataqualityFlag',
    'EventInputstat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity']
      for record in retVal['Value']:
        records += [[record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8],record[9],record[10],record[11],record[12],record[13],record[14],record[15]]]
      result = {'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)}
      fileString = cPickle.dumps(result, protocol=2)
      result = fileHelper.stringToNetwork(fileString)  
      if result['OK']:
        gLogger.info('Sent file %s of size %d' % (str(dict),len(fileString)))
      else:
        return result
    else:
      return retVal
    return S_OK()
  
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
  types_getProductionFiles = [IntType, StringType]
  def export_getProductionFiles(self, prod, fileType, replica='ALL'):
    return dataMGMT_.getProductionFiles(int(prod), fileType, replica)
  
  #############################################################################
  types_getFilesForAGivenProduction= [DictType]
  def export_getFilesForAGivenProduction(self, dict):
    return dataMGMT_.getFilesForAGivenProduction(dict)
  
  #############################################################################
  types_getAvailableRunNumbers = []
  def export_getAvailableRunNumbers(self):
    return dataMGMT_.getAvailableRunNumbers()
  
  #############################################################################
  types_getRunFiles = [IntType]
  def export_getRunFiles(self, runid):
    return dataMGMT_.getRunFiles(runid)
  
  #############################################################################
  types_updateFileMetaData = [StringType, DictType]
  def export_updateFileMetaData(self, filename, filesAttr):
    return dataMGMT_.updateFileMetaData(filename, filesAttr)
  
  #############################################################################
  types_renameFile = [StringType, StringType]
  def export_renameFile(self, oldLFN, newLFN):
    return dataMGMT_.renameFile(oldLFN, newLFN)
  
  #############################################################################
  types_getInputAndOutputJobFiles = [ListType]
  def export_getInputAndOutputJobFiles(self, jobids):
    return dataMGMT_.getInputAndOutputJobFiles(jobids)
  
  #############################################################################
  types_getProductionProcessingPassID = [LongType]
  def export_getProductionProcessingPassID(self, prodid):
    return dataMGMT_.getProductionProcessingPassID(prodid)
  
  #############################################################################
  types_getProductionProcessingPass = [LongType]
  def export_getProductionProcessingPass(self, prodid):
    return dataMGMT_.getProductionProcessingPass(prodid)
  
  #############################################################################
  types_getJobsIds = [ListType]
  def export_getJobsIds(self, filelist):
    return dataMGMT_.getJobsIds(filelist)
  
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
            faild[tag]=i
          else:
            successfull[tag]=i
        else:
          faild[tag]=i
    return S_OK({'Successfull':successfull, 'Faild':faild})
  
  #############################################################################  
  types_setQuality = [ListType, StringType]
  def export_setQuality(self, lfns, flag):
    return dataMGMT_.setQuality(lfns, flag)
  
  #############################################################################
  types_setRunQualityWithProcessing = [LongType,StringType,StringType]
  def export_setRunQualityWithProcessing(self, runNB, procpass, flag):
    return dataMGMT_.setRunQualityWithProcessing(runNB, procpass, flag)
  
  #############################################################################  
  types_setQualityRun = [IntType, StringType]
  def export_setQualityRun(self, runNb, flag):
    return dataMGMT_.setQualityRun(runNb, flag)
  
  #############################################################################  
  types_setQualityProduction = [IntType, StringType]
  def export_setQualityProduction(self, prod, flag):
    return dataMGMT_.setQualityProduction(prod, flag)
  
  #############################################################################
  types_getLFNsByProduction = [LongType]
  def export_getLFNsByProduction(self, prodid):
    return dataMGMT_.getLFNsByProduction(prodid)
  
  #############################################################################
  types_getAncestors = [ListType, IntType]
  def export_getAncestors(self, lfns, depth):
    return dataMGMT_.getAncestors(lfns, depth)
  
  #############################################################################
  types_getAllAncestors = [ListType, IntType]
  def export_getAllAncestors(self, lfns, depth):
    return dataMGMT_.getAllAncestors(lfns, depth)
  
  #############################################################################
  types_getAllAncestorsWithFileMetaData = [ListType, IntType]
  def export_getAllAncestorsWithFileMetaData(self, lfns, depth):
    return dataMGMT_.getAllAncestorsWithFileMetaData(lfns, depth)
  
  #############################################################################
  types_getAllDescendents = [ListType, IntType, IntType, BooleanType]
  def export_getAllDescendents(self, lfn, depth = 0, production=0, checkreplica=False):
    return dataMGMT_.getAllDescendents(lfn, depth, production, checkreplica)
  
  #############################################################################
  types_getDescendents = [ListType, IntType]
  def export_getDescendents(self, lfn, depth):
    return dataMGMT_.getDescendents(lfn, depth)
  
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
  types_insertJob =[DictType]
  def export_insertJob(self, job):
    return dataMGMT_.insertJob(job)
  
  #############################################################################
  types_insertInputFile = [LongType, LongType]
  def export_insertInputFile(self, jobID, FileId):
    return dataMGMT_.insertInputFile(jobID, FileId)
  
  #############################################################################
  types_insertOutputFile = [DictType]
  def export_insertOutputFile(self, file):
    return dataMGMT_.insertOutputFile(file)  
  
  #############################################################################
  types_updateReplicaRow = [LongType, StringType]
  def export_updateReplicaRow(self, fileID, replica):
    return dataMGMT_.updateReplicaRow(self, fileID, replica)
  
  types_deleteJob = [LongType]
  def export_deleteJob(self, job):
    return dataMGMT_.deleteJob(job)  
  
  #############################################################################
  types_deleteInputFiles = [LongType]
  def export_deleteInputFiles(self, jobid):
    return dataMGMT_.deleteInputFiles(long(jobid))
  
  #############################################################################
  types_deleteFiles = [ListType]
  def export_deleteFiles(self, lfns):
    return dataMGMT_.deleteFiles(lfns)
  
  #############################################################################
  types_insertSimConditions = [StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_insertSimConditions(self, simdesc,BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return dataMGMT_.insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
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
  def export_getFilesInformations(self,lfns):
    return dataMGMT_.getFilesInformations(lfns)
  
  #############################################################################  
  types_getFileMetaDataForUsers = [ListType]
  def export_getFileMetaDataForUsers(self, lfns):
    res = dataMGMT_.getFileMetaDataForUsers(lfns)
    return res
  
  #############################################################################  
  types_getProductionFilesForUsers = [IntType, DictType, DictType, LongType, LongType]
  def export_getProductionFilesForUsers(self, prod, ftype, SortDict, StartItem, Maxitems):
    res = dataMGMT_.getProductionFilesForUsers(prod, ftype, SortDict, StartItem, Maxitems)
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
  types_removeReplica = [StringType]
  def export_removeReplica(self, fileName):
    return dataMGMT_.removeReplica(fileName)
  
  #############################################################################
  types_getRunInformations = [IntType]
  def export_getRunInformations(self, runnb):
    return dataMGMT_.getRunInformations(runnb)
  
  #############################################################################
  types_getLogfile = [StringType]
  def export_getLogfile(self, lfn):
    return dataMGMT_.getLogfile(lfn)
  
  #############################################################################
  types_addEventType = [LongType, StringType, StringType]
  def export_addEventType(self,evid, desc, primary):
    result = dataMGMT_.checkEventType(evid)
    if not result['OK']:
      value = dataMGMT_.insertEventTypes(evid, desc, primary)
      if value['OK']:
        res = S_OK(str(evid)+' event type added successfully!')
      else:
        res = S_ERROR(value['Message'])
    else:
      return S_OK(str(evid)+' event type exists')
    return res
  
  #############################################################################
  types_updateEventType = [LongType, StringType, StringType]
  def export_updateEventType(self, evid, desc, primary):
    result = dataMGMT_.checkEventType(evid)
    if not result['OK']:
      return S_ERROR(str(evid) + ' event type is missing in the BKK database!')
    else:
      val = dataMGMT_.updateEventType(evid, desc, primary)
      if val['OK']:
        return S_OK(str(evid)+' event type updated successfully!')
      else:
        return S_ERROR(value['Message'])  
  
  #############################################################################
  types_addFiles = [ListType]
  def export_addFiles(self, lfns):
    result = {}
    for file in lfns:
      res = dataMGMT_.addReplica(file)
      if not res['OK']:
        result[file]= res['Message']
    return S_OK(result)
  
  #############################################################################
  types_removeFiles = [ListType]
  def export_removeFiles(self, lfns):
    result = {}
    for file in lfns:
      res = dataMGMT_.removeReplica(file)
      if not res['OK']:
        result[file]= res['Message']
    return S_OK(result)
  
  #############################################################################  
  types_getProductionSummary = [DictType]
  def export_getProductionSummary(self, dict):
    
    if dict.has_key('ConfigurationName'):
      cName = dict['ConfigurationName']
    else:
      cName = 'ALL'
    
    if dict.has_key('ConfigurationVersion'):
      cVersion = dict['ConfigurationVersion']
    else:
      cVersion = 'ALL'
    
    if dict.has_key('Production'):
      production = dict['Production']
    else:
      production = 'ALL'
      
    if dict.has_key('SimulationDescription'):
      simdesc = dict['SimulationDescription']
    else:
      simdesc = 'ALL'
    
    if dict.has_key('ProcessingPassGroup'):
      pgroup= dict['ProcessingPassGroup']
    else:
      pgroup= 'ALL'
      
    if dict.has_key('FileType'):
      ftype = dict['FileType']
    else:
      ftype = 'ALL'
    
    if dict.has_key('EventType'):
      evttype= dict['EventType'] 
    else:
      evttype='ALL'
    
    retVal = dataMGMT_.getProductionSummary(cName, cVersion, simdesc, pgroup, production, ftype, evttype)
          
    return retVal
  
  #############################################################################
  types_getProductionInformations_new = [LongType]
  def export_getProductionInformations_new(self, prodid):
    
    nbjobs = None
    nbOfFiles = None
    sizeofFiles = None
    nbOfEvents = None
    steps = None
    prodinfos = None 
    
    value = dataMGMT_.getJobsNb(prodid)
    if value['OK']==True:
      nbjobs = value['Value']
      
    value = dataMGMT_.getNbOfFiles(prodid)
    if value['OK']==True:
      nbOfFiles =value['Value']
      
    value = dataMGMT_.getSizeOfFiles(prodid)
    if value['OK']==True:
      sizeofFiles =  value['Value']
                 
    value = dataMGMT_.getNumberOfEvents(prodid)
    if value['OK']==True:
      nbOfEvents = value['Value']
    
    value = dataMGMT_.getConfigsAndEvtType(prodid)
    if value['OK']==True:
      prodinfos = value['Value']
    
    path = '/'
    
    if len(prodinfos) == 0:
      return S_ERROR('This production does not contains any jobs!')
    cname = prodinfos[0][0]
    cversion = prodinfos[0][1]
    path += cname+'/'+cversion+'/'
      
    value = dataMGMT_.getSteps(prodid)
    if value['OK']==True:
      steps = value['Value']
    else:
      steps = value['Message']
      result = {"Production informations":prodinfos,"Steps":steps,"Number of jobs":nbjobs,"Number of files":nbOfFiles,"Number of events":nbOfEvents, 'Path':path}
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
    prefix = '\n'+path
   
    for i in nbOfEvents:
      path += prefix + '/'+str(i[2])+'/'+i[0]
    result = {"Production informations":prodinfos,"Steps":steps,"Number of jobs":nbjobs,"Number of files":nbOfFiles,"Number of events":nbOfEvents, 'Path':path}
    return S_OK(result)
  
  #############################################################################
  types_getProductionInformationsFromView = [LongType]
  def export_getProductionInformationsFromView(self, prodid):
    value = dataMGMT_.getProductionInformationsFromView(prodid)
    parameters = []
    infos = []
    if value['OK']==True:
      records = value['Value']
      parameters = ['Production','EventTypeId','FileType','NumberOfEvents','NumberOfFiles']
      for record in records:
        infos += [[record[0],record[1],record[2], record[3], record[4]]]
    else:
      return S_ERROR(value['Message'])
    return S_OK({'ParameterNames':parameters,'Records':infos})
  
  
  #############################################################################
  types_getFileHistory = [StringType]
  def export_getFileHistory(self, lfn):
    retVal = dataMGMT_.getFileHistory(lfn)
    result = {}
    records = []
    if retVal['OK']:
      values = retVal['Value']
      parameterNames = ['FileId', 'FileName','ADLER32','CreationDate','EventStat','EventtypeId','Gotreplica', 'GUI', 'JobId', 'md5sum', 'FileSize', 'FullStat', 'Dataquality', 'FileInsertDate', 'Luminosity', 'InstLuminosity']
      sum = 0
      for record in values:
        value = [record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8],record[9],record[10],record[11],record[12],record[13], record[14], record[15]]
        records += [value]
        sum += 1
      result = {'ParameterNames':parameterNames,'Records':records,'TotalRecords':sum}
    else:
      result = S_ERROR(retVal['Message'])
    return S_OK(result)
  
  #############################################################################
  types_getJobsNb = [LongType]
  def export_getJobsNb(self, prodid):
    return dataMGMT_.getJobsNb(prodid)
  
  #############################################################################
  types_getNumberOfEvents = [LongType]
  def export_getNumberOfEvents(self, prodid):
    return dataMGMT_.getNumberOfEvents(prodid)
  
  #############################################################################
  types_getSizeOfFiles = [LongType]
  def export_getSizeOfFiles(self, prodid):
    return dataMGMT_.getSizeOfFiles(prodid)
  
  #############################################################################
  types_getNbOfFiles = [LongType]
  def export_getNbOfFiles(self, prodid):
    return dataMGMT_.getNbOfFiles(prodid)
  
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
    retVal = dataMGMT_.getProcessedEvents(prodid)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      result = retVal['Value'][0][0]
      return S_OK(result)
  
  #############################################################################
  types_getRunsWithAGivenDates = [DictType]
  def export_getRunsWithAGivenDates(self, dict):
    return dataMGMT_.getRunsWithAGivenDates(dict)
  
  #############################################################################
  types_getProductiosWithAGivenRunAndProcessing = [DictType]
  def export_getProductiosWithAGivenRunAndProcessing(self, dict):
    return dataMGMT_.getProductiosWithAGivenRunAndProcessing(dict)
  
  #############################################################################
  types_getDataQualityForRuns = [ListType]
  def export_getDataQualityForRuns(self, runs):
    return dataMGMT_.getDataQualityForRuns(runs)
      
  #############################################################################
  types_setFilesInvisible = [ListType]
  def export_setFilesInvisible(self, lfns):
    return dataMGMT_.setFilesInvisible(lfns)
  
  #############################################################################
  types_getRunFlag = [LongType, LongType]
  def export_getRunFlag(self, runnb, processing):
    return dataMGMT_.getRunFlag(runnb, processing)
  
  #############################################################################
  types_getAvailableConfigurations = []
  def export_getAvailableConfigurations(self):
    return dataMGMT_.getAvailableConfigurations()
   
   #############################################################################
  types_getRunProcessingPass=[LongType]
  def export_getRunProcessingPass(self, runnumber):
    return dataMGMT_.getRunProcessingPass(runnumber)
  
  #############################################################################
  types_checkProductionReplicas = [IntType]
  def export_checkProductionReplicas(self, productionid = None):
    return dataMGMT_.checkProductionStatus(productionid)
  
  #############################################################################
  types_checkLfns = [ListType]
  def export_checkLfns(self, lfns):
    return dataMGMT_.checkProductionStatus(productionid = None, lfns = lfns)
  
  #############################################################################  
  types_getFilesWithGivenDataSets = [DictType]
  def export_getFilesWithGivenDataSets(self, values):
    
    simdesc = 'ALL'
    if values.has_key('SimulationConditions'):
      simdesc = str(values['SimulationConditions'])   
    
    datataking = 'ALL'
    if values.has_key('DataTakingConditions'):
      datataking = str(values['DataTakingConditions'])
    
    if values.has_key('ProcessingPass'):
      procPass = values['ProcessingPass']
    else:
      procPass = 'ALL'
    
    if values.has_key('FileType'):
      ftype = values['FileType']
    else:
      return S_ERROR('FileType is missing!')
    
    if values.has_key('EventType'):
      evt = values['EventType']
    else:
      evt = 0
      
    if values.has_key('ConfigName'):
      configname = values['ConfigName']
    else:
      configname = 'ALL'
     
    if values.has_key('ConfigVersion'):
      configversion = values['ConfigVersion']
    else:
      configversion = 'ALL'
    
    if values.has_key('ProductionID'):
      prod = values['ProductionID']
      if prod == 0:
        prod = 'ALL'
    else:
      prod = 'ALL'
    
    if values.has_key('DataQualityFlag'):
      flag = values['DataQualityFlag']
    else:
      flag = 'ALL'
    
    if values.has_key('StartDate'):
      startd = values['StartDate']
    else:
      startd = None
    
    if values.has_key('EndDate'):
      endd = values['EndDate']
    else:
      endd = None
    if values.has_key('NbOfEvents'):
      nbofevents = values['NbOfEvents']
    else:
      nbofevents = False
    
    if values.has_key('StartRun'):
      startRunID = values['StartRun']
    else:
      startRunID = None
    
    if values.has_key('EndRun'):
      endRunID = values['EndRun']
    else:
      endRunID = None
    
    if values.has_key('RunNumbers'):
      runNbs = values['RunNumbers']
    else:
      runNbs = []
    
    replicaFlag = 'Yes'
    if values.has_key('ReplicaFlag'):
      replicaFlag = values['ReplicaFlag']
 
    result = []
    retVal = dataMGMT_.getFilesWithGivenDataSets(simdesc, datataking, procPass, ftype, evt, configname, configversion, prod, flag, startd, endd, nbofevents, startRunID, endRunID, runNbs, replicaFlag)
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
    
    simdesc = 'ALL'
    if values.has_key('SimulationConditions'):
      simdesc = str(values['SimulationConditions'])   
    
    datataking = 'ALL'
    if values.has_key('DataTakingConditions'):
      datataking = str(values['DataTakingConditions'])
    
    if values.has_key('ProcessingPass'):
      procPass = values['ProcessingPass']
    else:
      procPass = 'ALL'
    
    if values.has_key('FileType'):
      ftype = values['FileType']
    else:
      return S_ERROR('FileType is missing!')
    
    if values.has_key('EventType'):
      evt = values['EventType']
    else:
      evt = 0
      
    if values.has_key('ConfigName'):
      configname = values['ConfigName']
    else:
      configname = 'ALL'
     
    if values.has_key('ConfigVersion'):
      configversion = values['ConfigVersion']
    else:
      configversion = 'ALL'
    
    if values.has_key('ProductionID'):
      prod = values['ProductionID']
      if prod == 0:
        prod = 'ALL'
    else:
      prod = 'ALL'
    
    if values.has_key('DataQualityFlag'):
      flag = values['DataQualityFlag']
    else:
      flag = 'ALL'
    
    if values.has_key('StartDate'):
      startd = values['StartDate']
    else:
      startd = None
    
    if values.has_key('EndDate'):
      endd = values['EndDate']
    else:
      endd = None
    if values.has_key('NbOfEvents'):
      nbofevents = values['NbOfEvents']
    else:
      nbofevents = False
    
    if values.has_key('StartRun'):
      startRunID = values['StartRun']
    else:
      startRunID = None
    
    if values.has_key('EndRun'):
      endRunID = values['EndRun']
    else:
      endRunID = None
    
    if values.has_key('RunNumbers'):
      runNbs = values['RunNumbers']
    else:
      runNbs = []
    
    replicaFlag = 'Yes'
    if values.has_key('ReplicaFlag'):
      replicaFlag = values['ReplicaFlag'] 
    
    result = {}
    retVal = dataMGMT_.getFilesWithGivenDataSetsForUsers(simdesc, datataking, procPass, ftype, evt, configname, configversion, prod, flag, startd, endd, nbofevents, startRunID, endRunID, runNbs, replicaFlag)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
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
       nbfiles=+1
       if i[1] != None:
        nbevents+= i[1]
       if i[2] != None:
         evinput+= i[2]
       if i[5] != None:
         fsize+=i[5]
       if i[6] != None:
         tLumi += i[6]
       if i[7] != None:
         lumi += i[7]
       if i[8] != None:
         ilumi += i[8]
           
       result[i[0]] = {'EventStat':i[1],'EventInputStat':i[2],'Runnumber':i[3],'Fillnumber':i[4],'FileSize':i[5], 'TotalLuminosity':i[6],'Luminosity':i[7],'InstLuminosity':i[8]}  
    summary = {'Number Of Files':nbfiles,'Number of Events':nbevents,'EventInputStat':evinput,'FileSize':fsize/1000000000., 'TotalLuminosity':tLumi,'Luminosity':lumi,'InstLuminosity':ilumi}
    result['Summary']=summary
    return S_OK(result)
  
