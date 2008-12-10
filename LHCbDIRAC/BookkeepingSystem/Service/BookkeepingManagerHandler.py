########################################################################
# $Id: BookkeepingManagerHandler.py,v 1.84 2008/12/10 11:24:58 zmathe Exp $
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id: BookkeepingManagerHandler.py,v 1.84 2008/12/10 11:24:58 zmathe Exp $"

from types                                                                        import *
from DIRAC.Core.DISET.RequestHandler                                              import RequestHandler
from DIRAC                                                                        import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Service.copyFiles                                    import copyXMLfile
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager
from DIRAC.DataManagementSystem.Client.ReplicaManager                             import ReplicaManager
from DIRAC.Core.Utilities.Shifter                                                 import setupShifterProxyInEnv
from DIRAC.Core.Utilities                                                         import DEncode
import time,sys,os
import cPickle


global dataMGMT_
dataMGMT_ = BookkeepingDatabaseClient()

global reader_
reader_ = XMLFilesReaderManager()

def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
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
  types_filetransfer = [StringType, StringType]
  def export_filetransfer(self, name, data):
    try:
      gLogger.info("File Transfer: ", name)
      f = open('/opt/bookkeeping/XMLProcessing/ToDo1/'+name,'w')
      f.write(data)
      f.close()
      return S_OK("File Transfer fhinished successfully!")
    except Exception, x:
          print str(x)
          return S_ERROR('Error during processing '+name)
 

  #############################################################################
  #@@checking
  #############################################################################
  types_deleteJob = [LongType]
  def export_deleteJob(self, job):
    return dataMGMT_.deleteJob(job)  
  
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
  types_deleteInputFiles = [LongType]
  def export_deleteInputFiles(self, jobid):
    return dataMGMT_.deleteInputFiles(long(jobid))
  
  #############################################################################
  types_deleteFiles = [ListType]
  def export_deleteFiles(self, lfns):
    return dataMGMT_.deleteFiles(lfns)
  
   
  #############################################################################
  types_getSimulationCondID = [StringType, StringType, StringType, StringType, StringType, StringType]
  def export_getSimulationCondID(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return dataMGMT_.getSimulationCondID(BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
  #############################################################################
  types_insertSimConditions = [StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_insertSimConditions(self, simdesc,BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return dataMGMT_.insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
  #############################################################################
  types_getSimCondIDWhenFileName = [StringType]
  def export_getSimCondIDWhenFileName(self, fileName):
    return dataMGMT_.getSimCondIDWhenFileName(fileName)

  #############################################################################
  types_updateReplicaRow = [LongType, StringType]
  def export_updateReplicaRow(self, fileID, replica):
    return dataMGMT_.updateReplicaRow(self, fileID, replica)
  
  #############################################################################
  types_getSimConditions = []
  def export_getSimConditions(self):
    return dataMGMT_.getSimConditions()
  
  
  #############################################################################
  #@@ENDchecking
  #############################################################################
  
  types_getAvailableConfigurations = []
  def export_getAvailableConfigurations(self):
    return dataMGMT_.getAvailableConfigurations()
    
  #############################################################################
  types_getSimulationConditions = [StringType, StringType, IntType ]
  def export_getSimulationConditions(self, configName, configVersion, realdata):
    return dataMGMT_.getSimulationConditions(configName, configVersion, realdata)
  
  #############################################################################
  types_getProPassWithSimCond = [StringType, StringType, StringType]
  def export_getProPassWithSimCond(self, configName, configVersion, simcondid):
    return dataMGMT_.getProPassWithSimCond(configName, configVersion, simcondid)
  
  #############################################################################
  types_getEventTypeWithSimcond = [StringType, StringType, StringType, StringType]
  def export_getEventTypeWithSimcond(self,configName, configVersion, simcondid, procPass):
    return dataMGMT_.getEventTypeWithSimcond(configName, configVersion, simcondid, procPass)
  
  #############################################################################
  types_getProductionsWithSimcond = [StringType, StringType, StringType, StringType, StringType]
  def export_getProductionsWithSimcond(self, configName, configVersion, simcondid, procPass, evtId):
    return dataMGMT_.getProductionsWithSimcond(configName, configVersion, simcondid, procPass, evtId)
  
  #############################################################################
  types_getFileTypesWithSimcond = [StringType, StringType, StringType, StringType, StringType, StringType]
  def export_getFileTypesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod):
    return dataMGMT_.getFileTypesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod)
  
  #############################################################################  
  types_getProgramNameWithSimcond = [StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_getProgramNameWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype):
    return dataMGMT_.getProgramNameWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype)
  
  #############################################################################  
  types_getProductionFiles = [IntType, StringType]
  def export_getProductionFiles(self, prod, fileType):
    return dataMGMT_.getProductionFiles(int(prod), fileType)
  
  #############################################################################  
  types_getAvailableFileTypes = []
  def export_getAvailableFileTypes(self):
    res = dataMGMT_.getAvailableFileTypes()
    result = []
    if res['OK']:
      for record in res['Value']:
        element = [record[0]]
        result += [element]
    else:
      return res
    
    return S_OK(result)
  
  
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
  types_getFilesWithSimcond = [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    '''
    res = setupShifterProxyInEnv( "ProductionManager" )
    if not res[ 'OK' ]:
      gLogger.error( "Can't get shifter's proxy: %s" % res[ 'Message' ] )
      return res
    '''
    result = dataMGMT_.getFilesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion)
    return result
    '''
    if not result['OK']:
      return S_ERROR(result['Message'])
    
    files = result['Value'] 
    rm = ReplicaManager()
    list =[]
    for file in files:
      result = rm.getReplicas(file[0])
      value = result['Value']
      if len(value['Successful']) > 0:
        list += [file]
    return S_OK(list)
    '''  

    
  #############################################################################
  types_getLimitedFilesWithSimcond = [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, LongType, LongType]
  def export_getLimitedFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems ):
    result = dataMGMT_.getLimitedFilesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems)
    return result
  
  #############################################################################
  types_getLimitedNbOfFiles = [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_getLimitedNbOfFiles(self,configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    result = dataMGMT_.getLimitedNbOfFiles(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion)
    return result
  
  #############################################################################
  types_getSimCondWithEventType = [StringType, StringType, StringType, IntType]
  def export_getSimCondWithEventType(self, configName, configVersion, eventType, realdata):
    return dataMGMT_.getSimCondWithEventType(configName, configVersion, eventType, realdata)
  
  #############################################################################
  types_getProPassWithEventType = [StringType, StringType, StringType, StringType]
  def export_getProPassWithEventType(self, configName, configVersion, eventType, simcond):
    return dataMGMT_.getProPassWithEventType(configName, configVersion, eventType, simcond)
  
  
  #############################################################################
  types_getAncestors = [ListType, IntType]
  def export_getAncestors(self, lfns, depth):
    return dataMGMT_.getAncestors(lfns, depth)
  
  #############################################################################
  types_getEventTypes = [StringType, StringType]
  def export_getEventTypes(self, configName, configVersion):
    return dataMGMT_.getEventTypes(configName, configVersion)
   
  #############################################################################
  types_getSpecificFiles = [StringType, StringType, StringType, StringType, StringType, LongType, LongType]
  def export_getSpecificFiles(self, configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return dataMGMT_.getSpecificFiles(configName, configVersion, programName, programVersion, fileType, eventTypeId, production)
  
  #############################################################################  
  types_getPass_index = []
  def export_getPass_index(self):
    return dataMGMT_.getPass_index()
  
  #############################################################################  
  types_insert_pass_index = [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_insert_pass_index(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    return dataMGMT_.insert_pass_index(groupdesc, step0, step1, step2, step3, step4, step5, step6)
  
  #############################################################################  
  types_insertProcessing = [LongType, StringType, StringType, StringType]
  def export_insertProcessing(self, production, passdessc, inputprod, simcondsesc):
    return dataMGMT_.insertProcessing(production, passdessc, inputprod, simcondsesc)
  
  #############################################################################  
  types_listProcessingPass = [LongType]
  def export_listProcessingPass(self, prod=None):
    return dataMGMT_.listProcessingPass(prod)
  
  #############################################################################  
  types_listProcessingPass = []
  def listProcessingPass(self):
    return dataMGMT_.listProcessingPass(None)
  
  #############################################################################  
  types_getProductionsWithPocessingPass = [StringType]
  def export_getProductionsWithPocessingPass(self, processingPass):
    return dataMGMT_.getProductionsWithPocessingPass(processingPass)
  
  #############################################################################  
  types_getFilesByProduction = [LongType, LongType, StringType]
  def export_getFilesByProduction(self, production, eventtype, filetype):
    return dataMGMT_.getFilesByProduction(production, eventtype, filetype)
  
  #############################################################################  
  types_getProductions = [StringType, StringType, LongType]
  def export_getProductions(self, configName, configversion, eventTypeId):
    return dataMGMT_.getProductions(configName, configversion, long(eventTypeId))
  
  #############################################################################  
  types_getNumberOfEvents = [StringType, StringType, LongType, LongType]
  def export_getNumberOfEvents(self,configName, configversion, eventTypeId, production):
    return dataMGMT_.getNumberOfEvents(configName, configversion, eventTypeId, production)
  
  #############################################################################  
  types_getEventTyesWithProduction = [LongType]
  def export_getEventTyesWithProduction(self, production):
    return dataMGMT_.getEventTyesWithProduction(production)
  
  #############################################################################  
  types_getFileTypesWithProduction = [LongType, LongType]
  def export_getFileTypesWithProduction(self, production, eventType):
    return dataMGMT_.getFileTypesWithProduction(production, eventType)
  
  #############################################################################  
  types_getSpecificFilesWithoutProd = [StringType, StringType, StringType, StringType, StringType, LongType]
  def export_getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    return dataMGMT_.getSpecificFilesWithoutProd(configName, configVersion, pname, pversion, filetype, eventType)
  
  #############################################################################  
  types_getFileTypes = [StringType, StringType, LongType, LongType]
  def export_getFileTypes(self, configName, configVersion, eventType, prod):
    return dataMGMT_.getFileTypes( configName, configVersion, eventType, prod)

  #############################################################################  
  types_getProgramNameAndVersion = [StringType, StringType, LongType, LongType, StringType]
  def export_getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    return dataMGMT_.getProgramNameAndVersion(configName, configVersion, eventType, prod, fileType)
  
  #############################################################################
  types_getJobInfo = [StringType]
  def export_getJobInfo(self, lfn):
    return dataMGMT_.getJobInfo(lfn)
  
  # ----------------------------------Event Types------------------------------------------------------------------
  #############################################################################  
  types_getAvailableEventTypes = []
  def export_getAvailableEventTypes(self):
    return dataMGMT_.getAvailableEventTypes()
  
  #############################################################################  
  types_getConfigNameAndVersion = [LongType]
  def export_getConfigNameAndVersion(self, eventTypeId):
    return dataMGMT_.getConfigNameAndVersion(eventTypeId)
  
  #############################################################################  
  types_getAvailableProcessingPass = [StringType, StringType, LongType]
  def export_getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    return dataMGMT_.getAvailableProcessingPass(configName, configVersion, eventTypeId)

  #############################################################################
  types_getFileTypesWithEventType = [StringType, StringType, LongType, LongType]
  def export_getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    return dataMGMT_.getFileTypesWithEventType(configName, configVersion, eventTypeId, production)
  #############################################################################
  types_getFileTypesWithEventType = [StringType, StringType, LongType]
  def export_getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    return dataMGMT_.getFileTypesWithEventTypeALL(configName, configVersion, eventTypeId)

  #############################################################################
  types_getFilesByEventType = [StringType, StringType, StringType, LongType, LongType]
  def export_getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    return dataMGMT_.getFilesByEventType(configName, configVersion, fileType, eventTypeId, production)
  
  #############################################################################
  types_getFilesByEventType = [StringType, StringType, StringType, LongType]
  def export_getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    return dataMGMT_.getFilesByEventTypeALL(configName, configVersion, fileType, eventTypeId)
  
  #############################################################################
  types_getProductionsWithEventTypes = [LongType, StringType, StringType, StringType]
  def export_getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    return dataMGMT_.getProductionsWithEventTypes(eventType, configName,  configVersion, processingPass)
  
  #############################################################################
  types_addReplica = [StringType]
  def export_addReplica(self, fileName):
    return dataMGMT_.addReplica(fileName)
  
  #############################################################################
  types_removeReplica = [StringType]
  def export_removeReplica(self, fileName):
    return dataMGMT_.removeReplica(fileName)
  
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
  types_getLFNsByProduction = [LongType]
  def export_getLFNsByProduction(self, prodid):
    return dataMGMT_.getLFNsByProduction(prodid)
  
  #############################################################################
  types_checkProduction = [LongType]
  def export_checkProduction(self,prodid):  
    result = setupShifterProxyInEnv( "DataManager" )
    if not result[ 'OK' ]:
      gLogger.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
      return result
    rm = ReplicaManager()
    res = dataMGMT_.getLFNsByProduction(prodid)
    result = None
    if res['OK']:
      fileList = res['Value']
      list =[]
      for file in fileList:
        list +=[file[0]]
      result = rm.getReplicas(list)
    else:
      return S_ERROR(res['Message'])
    return result
  
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
  types_getFileMetadata = [ListType]
  def export_getFileMetadata(self, lfns):
    return dataMGMT_.getFileMetadata(lfns)
  
  #############################################################################
  types_exists = [ListType]
  def export_exists(self, lfns):
    return dataMGMT_.exists(lfns)
  
  #############################################################################
  types_updateFileMetaData = [StringType, DictType]
  def export_updateFileMetaData(self, filename, filesAttr):
    return dataMGMT_.updateFileMetaData(filename, filesAttr)
  
  '''
  Monitoring
  '''
  
  #############################################################################
  types_getProductionInformations = [LongType]
  def export_getProductionInformations(self, prodid):
    
    nbjobs = None
    nbOfFiles = None
    sizeofFiles = None
    nbOfEvents = None
    prodinfo = None
    
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
      
    value = dataMGMT_.getProductionInformation(prodid)
    if value['OK']==True:
      prodinfo = value['Value']
  
    result = {"Production Info":prodinfo,"Number Of jobs":nbjobs,"Number Of files":nbOfFiles,"Number of Events":nbOfEvents}
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
  types_getProcessingPassGroups = []
  def export_getProcessingPassGroups(self):
     return dataMGMT_.getProcessingPassGroups()
  
  #############################################################################
  types_insert_pass_group = [StringType]
  def export_insert_pass_group(self, gropupdesc):
    return dataMGMT_.insert_pass_group(gropupdesc)
    
  #############################################################################
  types_renameFile = [StringType, StringType]
  def export_renameFile(self, oldLFN, newLFN):
    return dataMGMT_.renameFile(oldLFN, newLFN)
  
  #############################################################################
  types_getJobsIds = [ListType]
  def export_getJobsIds(self, filelist):
    return dataMGMT_.getJobsIds(filelist)
  
  #############################################################################
  types_getInputAndOutputJobFiles = [ListType]
  def export_getInputAndOutputJobFiles(self, jobids):
    return dataMGMT_.getInputAndOutputJobFiles(jobids)
    
    
  
  '''
  End Monitoring
  '''
  #############################################################################
  def transfer_toClient( self, parametes, token, fileHelper ):
    select = parametes.split('>')
    result = dataMGMT_.getFilesWithSimcond(select[0], select[1], select[2], select[3], select[4], select[5], select[6], select[7], select[8])
    if not result['OK']:
      return S_ERROR(result['Message'])
    fileString = cPickle.dumps(result['Value'], protocol=2)
    #fileString = DEncode.encode(result['Value'])
    result = fileHelper.stringToNetwork(fileString)  
    if result['OK']:
      gLogger.info('Sent file %s of size %d' % (parametes,len(fileString)))
    else:
      return result
    return S_OK()
    #-----------------------------------END Event Types------------------------------------------------------------------