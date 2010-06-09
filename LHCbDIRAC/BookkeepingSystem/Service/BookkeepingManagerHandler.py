########################################################################
# $Id$
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager
from LHCbDIRAC.BookkeepingSystem.Service.copyFiles                                    import copyXMLfile

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
  #@@ENDchecking
  #############################################################################
  
  types_getAvailableConfigurations = []
  def export_getAvailableConfigurations(self):
    return dataMGMT_.getAvailableConfigurations()
   
  #############################################################################
  types_getAvailableProductions = []
  def export_getAvailableProductions(self): 
    return dataMGMT_.getAvailableProductions()
  
  #############################################################################
  types_getAvailableConfigNames = []
  def export_getAvailableConfigNames(self):
    return dataMGMT_.getAvailableConfigNames()
  
  #############################################################################
  types_getConfigVersions = [StringType]
  def export_getConfigVersions(self, configname):
    return dataMGMT_.getConfigVersions(configname)


  #############################################################################
  types_getSimulationConditions = [StringType, StringType, IntType ]
  def export_getSimulationConditions(self, configName, configVersion, realdata):
    return dataMGMT_.getSimulationConditions(configName, configVersion, realdata)
  
  #############################################################################
  types_getProPassWithSimCondNew = [StringType, StringType, StringType]
  def export_getProPassWithSimCondNew(self, configName, configVersion, simcondid):
    return dataMGMT_.getProPassWithSimCondNew(configName, configVersion, simcondid)
  
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
  def export_getProductionFiles(self, prod, fileType, replica='ALL'):
    return dataMGMT_.getProductionFiles(int(prod), fileType, replica)
  
  #############################################################################
  types_getProductionFilesWithAGivenDate = [DictType]
  def export_getProductionFilesWithAGivenDate(self, dataSet):
    if dataSet.has_key('Production'):
      production = dataSet['Production']
    else:
      return S_ERROR('The dictionary is not contains the Production!')
    
    if dataSet.has_key('FileType'):
      fileType = dataSet['FileType']
    else:
      fileType = 'ALL'
    
    if dataSet.has_key('StartDate'):
      sDate = dataSet['StartDate']
    else:
      sDate = None 
     
    if dataSet.has_key('EndDate'):
      eDate = dataSet['EndDate']
    else:
      eDate = None 
    
    return dataMGMT_.getProductionFilesWithAGivenDate(production, fileType, sDate, eDate)
  
  #############################################################################  
  types_getProcessingPassDesc = [StringType, IntType, StringType]
  def export_getProcessingPassDesc(self, totalproc, passid, simid='ALL'):
    return dataMGMT_.getProcessingPassDesc(totalproc, passid, simid)
  
  types_getProcessingPassDesc_new = [StringType, StringType]
  def export_getProcessingPassDesc_new(self, totalproc, simid='ALL'):
    return dataMGMT_.getProcessingPassDesc_new(totalproc, simid)
  
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
  types_getProPassWithEventTypeNew = [StringType, StringType, StringType, StringType]
  def export_getProPassWithEventTypeNew(self, configName, configVersion, eventType, simcond):
    return dataMGMT_.getProPassWithEventTypeNew(configName, configVersion, eventType, simcond)
  
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
  types_getDescendents = [ListType, IntType]
  def export_getDescendents(self, lfn, depth):
    return dataMGMT_.getDescendents(lfn, depth)
  
  #############################################################################
  types_getAllDescendents = [ListType, IntType, IntType, BooleanType]
  def export_getAllDescendents(self, lfn, depth = 0, production=0, checkreplica=False):
    return dataMGMT_.getAllDescendents(lfn, depth, production, checkreplica)
  
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
  types_insert_pass_index_new = [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_insert_pass_index_new(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    return dataMGMT_.insert_pass_index_new(groupdesc, step0, step1, step2, step3, step4, step5, step6)
  
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
      
    result = []
    retVal = dataMGMT_.getFilesWithGivenDataSets(simdesc, datataking, procPass, ftype, evt, configname, configversion, prod, flag, startd, endd, nbofevents, startRunID, endRunID, runNbs)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      values = retVal['Value']
      for i in values:
        result += [i[0]]

    return S_OK(result)
  
  types_getProcessedEvents = [IntType]
  def export_getProcessedEvents(self, prodid):
    retVal = dataMGMT_.getProcessedEvents(prodid)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      result = retVal['Value'][0][0]
      return S_OK(result)

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
  
  #############################################################################
  types_getAvailableRuns = []
  def export_getAvailableRuns(self):
    return dataMGMT_.getAvailableRuns()
  
  #############################################################################
  types_checkAddProduction = [DictType]
  def export_checkAddProduction(self, infos):
    gLogger.debug(infos)
    result = ''
    if not infos.has_key('Steps'):
      result += "Missing Steps!\n"
    if not infos.has_key('GroupDescription'):
      result += "Missing Group Description!\n"
    if not infos.has_key('SimulationConditions'):
      result += "Missing Simulation Conditions!\n"
    if not infos.has_key('Production'):
      result += "Production is missing!\n"
    if result == '':
      steps = infos['Steps']
      groupdesc = infos['GroupDescription']
      simcond = infos['SimulationConditions']
      inputProdTotalProcessingPass = ''
      production = infos['Production']
      if infos.has_key('InputProductionTotalProcessingPass'):
        inputProdTotalProcessingPass = infos['InputProductionTotalProcessingPass']
      retVal = dataMGMT_.checkProcessingPassAndSimCond(production)
      if not retVal['OK']:
        result += retVal['Message']+'\n'
      else:
        value = retVal['Value']
        if value[0][0] != 0:
          result += 'The production is exist in the productions table!'          

    if result=='':
      result += ' Production: '+str(production)+'\n'
      result += 'Group description: '+str(groupdesc) +'\n'
      result += 'Simulation conditions: '+str(simcond) +'\n\n'
      result += 'Input production total processing pass: '+ str(inputProdTotalProcessingPass) +'\n'
      retVal  = dataMGMT_.checkAddProduction(steps, groupdesc, simcond, inputProdTotalProcessingPass, production)
      if retVal['OK']:
        result += retVal['Value']
      else:
         result += retVal['Value']
    return S_OK(result)
      
  #############################################################################
  types_addProduction = [DictType]
  '''
  infos is a dictionary. It contains: PrgNamesVersions,  GroupDescription, SimulationConditions, Production, InputProductionTotalProcessingPass
  for example: cl.addProduction({'PrgNamesVersions':{'Step0':'Gauss-v35r1','Step1':'Boole-v16r3','Step2':'Brunel-v33r3'},'GroupDescription':'ParticleGun','SimulationConditions':simcond,'Production':12})
               
  
  PrgNamesVersions: program names and versions are used this production
  GroupDescription: wich group correspond this processing ex: DC06-Sim or ParticleGun ....
  SimulationCondition: ex. simcond = {'BeamEnergy': 'rerer', 'Generator': 'dsds', 'Luminosity': 'wwww', 'MagneticField': 'hhh', 'BeamCond': 'sasas', 'DetectorCond': 'ddfd', 'SimDescription': 'Proba'}
  InputProductionTotalProcessingPass: we have to know the input production total production ex: {'InputProductionTotalProcessingPass':'MC08-SIM-Reco_v33'}
  '''
  def export_addProduction(self, infos):
    gLogger.debug(infos)
    result = None
    
    simcond = None
    daqdesc = None
    ok = False
    if infos.has_key('SimulationConditions'):
      simcond = infos['SimulationConditions']
      ok = True
    
    if infos.has_key('DataTakingConditions'):
      daqdesc = infos['DataTakingConditions']
      ok = True
    
    if not ok:
      result = S_ERROR('SimulationConditions or DataTakingConditins is missing!')
    
    
    if not infos.has_key('Steps'):
      result = S_ERROR("Missing Steps!")
    if not infos.has_key('GroupDescription'):
      result = S_ERROR("Missing Group Description!")
    if not infos.has_key('Production'):
      result = S_ERROR('Production is missing!')
    if not result:
      steps = infos['Steps']
      groupdesc = infos['GroupDescription']
      inputProdTotalProcessingPass = ''
      production = infos['Production']
      if infos.has_key('InputProductionTotalProcessingPass'):
        inputProdTotalProcessingPass = infos['InputProductionTotalProcessingPass']
      
      if simcond != None:
        res = dataMGMT_.insert_procressing_pass(steps, groupdesc, simcond, inputProdTotalProcessingPass, production)
        if res['OK']:
          result = S_OK('Processing pass succesfull defined!')
        else:
          result = res
      elif daqdesc != None:
        res = dataMGMT_.insert_procressing_passRealData(steps, groupdesc, daqdesc, inputProdTotalProcessingPass, production)
        if res['OK']:
          result = S_OK('Processing pass succesfull defined!')
        else:
          result = res
    return result
  
  #############################################################################  
  types_setQuality = [ListType, StringType]
  def export_setQuality(self, lfns, flag):
    return dataMGMT_.setQuality(lfns, flag)
  
  #############################################################################  
  types_setQualityRun = [IntType, StringType]
  def export_setQualityRun(self, runNb, flag):
    return dataMGMT_.setQualityRun(runNb, flag)
  
  #############################################################################  
  types_setQualityProduction = [IntType, StringType]
  def export_setQualityProduction(self, prod, flag):
    return dataMGMT_.setQualityProduction(prod, flag)
  
  #############################################################################  
  types_getAvailableDataQuality = []
  def export_getAvailableDataQuality(self):
    return dataMGMT_.getAvailableDataQuality()
  
  #############################################################################  
  types_insert_aplications = [StringType, StringType, StringType, StringType, StringType, StringType]
  def export_insert_aplications(self, appName, appVersion, option, dddb, condb, extrapack):
    return dataMGMT_.insert_aplications(appName, appVersion, option, dddb, condb, extrapack)
  
  #############################################################################  
  types_insert_pass_index_migration = [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_insert_pass_index_migration(self, passid, descr, groupid, step0,step1, step2,step3,step4,step5,step6):
    return dataMGMT_.insert_pass_index_migration(passid, descr, groupid, step0,step1, step2,step3,step4,step5,step6)
  
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
  types_getLogfile = [StringType]
  def export_getLogfile(self, lfn):
    return dataMGMT_.getLogfile(lfn)
  
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
  types_getFilesInformations = [ListType]
  def export_getFilesInformations(self,lfns):
    return dataMGMT_.getFilesInformations(lfns)
  
  #############################################################################
  types_getProductionsWithPrgAndEvt = [StringType, StringType, StringType]
  def export_getProductionsWithPrgAndEvt(self, programName='ALL', programversion='ALL', evt='ALL'):
    return dataMGMT_.getProductionsWithPrgAndEvt(programName, programversion, evt)
  
  #############################################################################
  types_exists = [ListType]
  def export_exists(self, lfns):
    return dataMGMT_.exists(lfns)
  
  #############################################################################
  types_updateFileMetaData = [StringType, DictType]
  def export_updateFileMetaData(self, filename, filesAttr):
    return dataMGMT_.updateFileMetaData(filename, filesAttr)
  
  #############################################################################
  types_getRunInformations = [IntType]
  def export_getRunInformations(self, runnb):
    return dataMGMT_.getRunInformations(runnb)
  
  #############################################################################
  types_getRunFiles = [IntType]
  def export_getRunFiles(self, runid):
    return dataMGMT_.getRunFiles(runid)
  
  #############################################################################
  types_checkProductionReplicas = [IntType]
  def export_checkProductionReplicas(self, productionid = None):
    return dataMGMT_.checkProductionStatus(productionid)
  
  #############################################################################
  types_checkLfns = [ListType]
  def export_checkLfns(self, lfns):
    return dataMGMT_.checkProductionStatus(productionid = None, lfns = lfns)
  
  #############################################################################
  types_getAvailableRunNumbers = []
  def export_getAvailableRunNumbers(self):
    return dataMGMT_.getAvailableRunNumbers()
  
  #############################################################################
  types_getProPassWithRunNumber = [StringType]
  def export_getProPassWithRunNumber(self, runnumber):
    return dataMGMT_.getProPassWithRunNumber(runnumber)
  
  #############################################################################
  types_getEventTypeWithAgivenRuns = [StringType, StringType]
  def export_getEventTypeWithAgivenRuns(self, runnumber, processing):
    return dataMGMT_.getEventTypeWithAgivenRuns(runnumber, processing)
  
  #############################################################################
  types_getFileTypesWithAgivenRun = [StringType, StringType, StringType]
  def export_getFileTypesWithAgivenRun(self, runnumber, procPass, evtId):
    return dataMGMT_.getFileTypesWithAgivenRun(runnumber, procPass, evtId)
  
  #############################################################################
  types_getLimitedNbOfRunFiles = [StringType, StringType, StringType, StringType]
  def export_getLimitedNbOfRunFiles(self,  procPass, evtId, runnumber, ftype):
    return dataMGMT_.getLimitedNbOfRunFiles(procPass, evtId, runnumber, ftype)
  
  #############################################################################
  types_getLimitedFilesWithAgivenRun = [StringType, StringType, StringType, StringType, IntType, IntType]
  def export_getLimitedFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype, startitem, maxitems):
    return dataMGMT_.getLimitedFilesWithAgivenRun(procPass, evtId, runnumber, ftype, startitem, maxitems)
  
  #############################################################################
  types_getRunFilesWithAgivenRun = [StringType, StringType, StringType, StringType]
  def export_getRunFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype):
    return dataMGMT_.getRunFilesWithAgivenRun(procPass, evtId, runnumber, ftype)
  
  #############################################################################
  types_getFileHistory = [StringType]
  def export_getFileHistory(self, lfn):
    retVal = dataMGMT_.getFileHistory(lfn)
    result = {}
    records = []
    if retVal['OK']:
      values = retVal['Value']
      parameterNames = ['FileId', 'FileName','ADLER32','CreationDate','EventStat','EventtypeId','Gotreplica', 'GUI', 'JobId', 'md5sum', 'FileSize', 'FullStat', 'Dataquality', 'FileInsertDate']
      sum = 0
      for record in values:
        value = [record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8],record[9],record[10],record[11],record[12],record[13]]
        records += [value]
        sum += 1
      result = {'ParameterNames':parameterNames,'Records':records,'TotalRecords':sum}
    else:
      result = S_ERROR(retVal['Message'])
    return S_OK(result)
  
  #############################################################################
  
  '''
  Monitoring
  '''
  
  #############################################################################
  types_getMoreProductionInformations = [IntType]
  def export_getMoreProductionInformations(self, prodid):
    return dataMGMT_.getMoreProductionInformations(prodid)
  
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
    res = dataMGMT_.getProductionProcessing(prodid)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      path += '/'+res['Value']
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
  types_getLimitedFilesWithSimcondAndDataQuality = [DictType]
  def export_getLimitedFilesWithSimcondAndDataQuality(self, dict):
        
    if dict.has_key('CName'):
      configName = dict['CName']
    if dict.has_key('CVersion'):
      configVersion = dict['CVersion']
    if dict.has_key('Simid'):
      simcondid = dict['Simid']
    if dict.has_key('Ppass'):
      procPass = dict['Ppass']
    if dict.has_key('Etype'):
      evtId = dict['Etype']
    if dict.has_key('Prod'):
      prod = dict['Prod']
    if dict.has_key('Ftype'):
      ftype = dict['Ftype']
    if dict.has_key('Pname'):
      progName = dict['Pname']
    if dict.has_key('Pversion'):
      progVersion = dict['Pversion']
    if dict.has_key('Sitem'):
      startitem = dict['Sitem']
    if dict.has_key('Mitem'):
      maxitems = dict['Mitem']
    if dict.has_key('Quality'):
      quality = dict['Quality']
      
    retVal = dataMGMT_.getLimitedFilesWithSimcondAndDataQuality(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems, quality)
    return retVal
  
  #############################################################################
  types_getRunFilesWithAgivenRunWithDataQuality = [DictType]
  def export_getRunFilesWithAgivenRunWithDataQuality(self, dict):
     
    if dict.has_key('Ppass'):
      procPass = dict['Ppass']
    if dict.has_key('Evid'):
      evtId = dict['Evid']
    if dict.has_key('Rnumber'): 
      runnumber = dict['Rnumber']
    if dict.has_key('Ftye'):
      ftype = dict['Ftye']
    if dict.has_key('Quality'):
      quality = dict['Quality']
    retVal = dataMGMT_.getRunFilesWithAgivenRunWithDataQuality(procPass, evtId, runnumber, ftype, quality)
    return retVal
  
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
    
    result = {"Production Info":prodinfo,"Number of jobs":nbjobs,"Number of files":nbOfFiles,"Number of events":nbOfEvents}
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
  
  #############################################################################
  types_getProcessingPassDescfromProduction = [IntType]
  def export_getProcessingPassDescfromProduction(self, prod):
    return dataMGMT_.getProcessingPassDescfromProduction(prod)
  
  #############################################################################
  types_getAvailableFileTypes = []
  def export_getAvailableFileTypes(self):
    return dataMGMT_.getAvailableFileTypes()
  
  #############################################################################
  types_insertFileTypes = [StringType, StringType]
  def export_insertFileTypes(self, ftype, desc):
    return dataMGMT_.insertFileTypes(ftype, desc)
  
  #############################################################################
  types_getAvailableTags = []
  def export_getAvailableTags(self):
    return dataMGMT_.getAvailableTags()
  
  #############################################################################
  types_getRunsWithAGivenDates = [DictType]
  def export_getRunsWithAGivenDates(self, dict):
    return dataMGMT_.getRunsWithAGivenDates(dict)
    
  '''
  End Monitoring
  '''
  #############################################################################
  def transfer_toClient( self, parametes, token, fileHelper ):
    select = parametes.split('>')
    print select
    if len(select)>9:
      result = dataMGMT_.getFilesWithSimcondAndDataQuality(select[0], select[1], select[2], select[3], select[4], select[5], select[6], select[7], select[8], select[9].split(';')[1:])
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
    else:
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
