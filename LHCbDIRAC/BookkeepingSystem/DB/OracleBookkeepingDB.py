########################################################################
# $Id: OracleBookkeepingDB.py,v 1.21 2008/08/21 14:18:25 zmathe Exp $
########################################################################
"""

"""

__RCSID__ = "$Id: OracleBookkeepingDB.py,v 1.21 2008/08/21 14:18:25 zmathe Exp $"

from types                                                           import *
from DIRAC.BookkeepingSystem.DB.IBookkeepingDB                       import IBookkeepingDB
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
import datetime
class OracleBookkeepingDB(IBookkeepingDB):
  
  #############################################################################
  def __init__(self):
    """
    """
    super(OracleBookkeepingDB, self).__init__()
    #self.user_ = gConfig.getValue("userName", "LHCB_BOOKKEEPING_INT")
    #self.password_ = gConfig.getValue("password", "Ginevra2008")
    #self.tns_ = gConfig.getValue("tns", "int12r")
   
    self.db_ = OracleDB("LHCB_BOOKKEEPING_INT_W", "Ginevra2008", "int12r")
  
  #############################################################################
  def getAvailableConfigurations(self):
    """
    """
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAvailableConfigurations',[])

  #############################################################################
  def getSimulationConditions(self, configName, configVersion):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getSimulationConditions', [configName, configVersion])
  
  #############################################################################
  def getProPassWithSimCond(self, configName, configVersion, simcondid):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProPassWithSimCond', [configName, configVersion, simcondid])
  
  #############################################################################
  def getEventTypeWithSimcond(self,configName, configVersion, simcondid, procPass):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTypeWithSimcond', [configName, configVersion, simcondid, procPass])
  
  #############################################################################
  def getProductionsWithSimcond(self, configName, configVersion, simcondid, procPass, evtId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProductionsWithSimcond', [configName, configVersion, simcondid, procPass, evtId])
  
  #############################################################################
  def getFileTypesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithSimcond', [configName, configVersion, simcondid, procPass, evtId, prod])
  
  #############################################################################  
  def getProgramNameWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProgramNameWithSimcond', [configName, configVersion, simcondid, procPass, evtId, prod, ftype])
  
  #############################################################################  
  def getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFilesWithSimcond', [configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion])
    
    
    
    
    
    
    
  #############################################################################
  def getAvailableEventTypes(self):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTypes', [configName, configVersion])
  
  #############################################################################
  def getSpecificFiles(self,configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getSpecificFiles', [configName, configVersion, programName, programVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getProcessingPass(self):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProcessingPass', [])
  
  #############################################################################  
  def getProductionsWithPocessingPass(self, processingPass):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProductions', [processingPass])
  
  #############################################################################  
  def getFilesByProduction(self, production, eventtype, filetype):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFilesByProduction', [production, eventtype, filetype])
  
  #############################################################################
  def getProductions(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProductions', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getNumberOfEvents(self, configName, configversion, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getNumberOfEvents', [configName, configversion, eventTypeId, production])
  
  #############################################################################
  def getEventTyesWithProduction(self, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTyesWithProduction', [production])
  
  #############################################################################  
  def getFileTypesWithProduction(self, production, eventType):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithProduction', [production, eventType])
  
  #############################################################################  
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getSpecificFilesWithoutProd',[configName,configVersion, pname, pversion, filetype, eventType])
  
  #############################################################################  
  def getFileTypes(self, configName, configVersion, eventType, prod):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypes',[configName, configVersion, eventType, prod]) 
  
  #############################################################################  
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProgramNameAndVersion', [configName, configVersion, eventType, prod, fileType])
  
  #############################################################################  
  def getAvailableEventTypes(self):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  #############################################################################  
  def getConfigNameAndVersion(self, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getConfigNameAndVersion', [eventTypeId])
  
  #############################################################################  
  def getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAvailableProcessingPass', [configName, configVersion, eventTypeId])

  #############################################################################
  def getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId, production])
  
  #############################################################################
  def getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId])
  
  #############################################################################
  def getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProductionsWithEventTypes', [eventType, configName,  configVersion, processingPass])
  
  #############################################################################
  def getSimulationCondID(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.db_.executeStoredFunctions('BKK_ORACLE.getSimulationCondID', LongType, [BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity])
  
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    return self.db_.executeStoredFunctions('BKK_ORACLE.getSimCondIDWhenFileName', LongType, [fileName])

  #############################################################################  
  def getLFNsByProduction(self, prodid):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getLFNsByProduction',[prodid])
  
  #############################################################################  
  def getAncestors(self, lfn, depth):
    logicalFileNames={}
    jobsId = []
    job_id = -1
    deptpTmp = depth
    for fileName in lfn:
      jobsId = []
      result = self.db_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        job_id = int(result['Value'])
      jobsId = [job_id]
      files = []
      depthTmp = depth
      while (depthTmp-1) and jobsId:
         for job_id in jobsId:
           command = 'select files.fileName,files.jobid from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)
           jobsId=[]
           res = self.db_._query(command)
           if not res['OK']:
             gLogger.error('Ancestor',result["Message"])
           else:
             dbResult = res['Value']
             for record in dbResult:
               jobsId +=[record[1]]
               files += [record[0]]
         depth-=1 
      logicalFileNames[fileName]=files    
    return logicalFileNames
  
    '''
    logicalFileNames=lfn
    jobsId = []
    id = -1
    for fileName in lfn:
      result = self.db_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        id = int(result['Value'])
      jobsId += [id]
     
    command=''
    while (depth-1) and jobsId:
         command = 'select files.fileName,files.jobid from inputfiles,files where '
         for job_id in jobsId:
             command +='inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)+' or '
         command=command[:-3]
         jobsId=[]
         res = self.db_._query(command)
         if not res['OK']:
           gLogger.error('Ancestor',result["Message"])
         else:
           dbResult = res['Value']
           for record in dbResult:
             jobsId +=[record[1]]
             logicalFileNames += [record[0]]
         depth-=1     
    return logicalFileNames
   '''
  
  #############################################################################  
  def getReverseAncestors(self, lfn, depth):
    logicalFileNames={}
    jobsId = []
    job_id = -1
    deptpTmp = depth
    for fileName in lfn:
      jobsId = []
      result = self.db_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        job_id = int(result['Value'])
      jobsId = [job_id]
      files = []
      depthTmp = depth
      while (depthTmp-1) and jobsId:
         for job_id in jobsId:
           command = 'select files.fileName,files.jobid from inputfiles,files where inputfiles.fileid=files.fileid and files.jobid='+str(job_id)
           jobsId=[]
           res = self.db_._query(command)
           if not res['OK']:
             gLogger.error('Ancestor',result["Message"])
           else:
             dbResult = res['Value']
             for record in dbResult:
               jobsId +=[record[1]]
               files += [record[0]]
         depth-=1 
      logicalFileNames[fileName]=files    
    return logicalFileNames
  
  """
  data insertation into the database
  """
  #############################################################################
  def checkfile(self, fileName): #file

    result = self.db_.executeStoredProcedure('BKK_ORACLE.checkfile',[fileName])['Value']
    if len(result)!=0:
      return S_OK(result)
    else:
      gLogger.error("File not found! ",str(fileName))
      return S_ERROR("File not found!"+str(fileName))
    
    return result
  
  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    result = self.db_.executeStoredProcedure('BKK_ORACLE.checkFileTypeAndVersion',[type, version])['Value']
    if len(result)!=0:
      return S_OK(result)
    else:
      gLogger.error("File type not found! ",str(type))
      return S_ERROR("File type not found!"+str(type))
    
    return result

    
  
  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    
    result = self.db_.executeStoredProcedure('BKK_ORACLE.checkEventType',[eventTypeId])['Value']
    if len(result)!=0:
      return S_OK(result)
    else:
      gLogger.error("Event type not found! ",str(eventTypeId))
      return S_ERROR("Event type not found!"+str(eventTypeId))
    
    return result
  
  #############################################################################
  def insertJob(self, job):
    
    gLogger.info("Insert job into database!")
    attrList = {'ConfigName':None, \
                 'ConfigVersion':None, \
                 'DAQPeriodId':None, \
                 'DiracJobId':None, \
                 'DiracVersion':None, \
                 'EventInputStat':None, \
                 'ExecTime':None, \
                 'FirstEventNumber':None, \
                 'Generator':None, \
                 'GeometryVersion':None, \
                 'GridJobId':None, \
                 'JobEnd':None, \
                 'JobStart':None, \
                 'LocalJobId':None, \
                 'Location':None, \
                 'LuminosityEnd':None, \
                 'LuminosityStart':None, \
                 'Name':None, \
                 'NumberOfEvents':None, \
                 'Production':None, \
                 'ProgramName':None, \
                 'ProgramVersion':None, \
                 'StatisticsRequested':None, \
                 'WNCPUPower':None, \
                 'WNCPUTime':None, \
                 'WNCache':None, \
                 'WNMemory':None, \
                 'WNModel':None, \
                 'WorkerNode':None}
    
    
    for param in job:
      if not attrList.__contains__(param):
        gLogger.error("insert job error: "," the job table not contains "+param+" this attributte!!")
        return S_ERROR(" The job table not contains "+param+" this attributte!!")
      attrList[param] = job[param]
    
      
    result = self.db_.executeStoredFunctions('BKK_ORACLE.insertJobsRow',LongType,[ attrList['ConfigName'], attrList['ConfigVersion'], \
                  attrList['DAQPeriodId'], \
                  attrList['DiracJobId'], \
                  attrList['DiracVersion'], \
                  attrList['EventInputStat'], \
                  attrList['ExecTime'], \
                  attrList['FirstEventNumber'], \
                  attrList['Generator'], \
                  attrList['GeometryVersion'], \
                  attrList['GridJobId'], \
                  attrList['JobEnd'], \
                  attrList['JobStart'], \
                  attrList['LocalJobId'], \
                  attrList['Location'], \
                  attrList['LuminosityEnd'], \
                  attrList['LuminosityStart'], \
                  attrList['Name'], \
                  attrList['NumberOfEvents'], \
                  attrList['Production'], \
                  attrList['ProgramName'], \
                  attrList['ProgramVersion'], \
                  attrList['StatisticsRequested'], \
                  attrList['WNCPUPower'], \
                  attrList['WNCPUTime'], \
                  attrList['WNCache'], \
                  attrList['WNMemory'], \
                  attrList['WNModel'], \
                  attrList['WorkerNode'] ])           
    return result
  
  #############################################################################
  def insertInputFile(self, jobID, FileId):
    result = self.db_.executeStoredProcedure('BKK_ORACLE.insertInputFilesRow',[FileId, jobID], False)
    return result
  #############################################################################
  def insertOutputFile(self, file):
  
      attrList = {  'Adler32':None, \
                    'CreationDate':None, \
                    'EventStat':None, \
                    'EventTypeId':None, \
                    'FileName':None,  \
                    'FileTypeId':None, \
                    'GotReplica':None, \
                    'Guid':None,  \
                    'JobId':None, \
                    'MD5Sum':None, \
                    'FileSize':None }
      
      for param in file:
        if not attrList.__contains__(param):
          gLogger.error("insert file error: "," the files table not contains "+param+" this attributte!!")
          return S_ERROR(" The files table not contains "+param+" this attributte!!")
        attrList[param] = file[param]
      
      result = self.db_.executeStoredFunctions('BKK_ORACLE.insertFilesRow',LongType, [  attrList['Adler32'], \
                    attrList['CreationDate'], \
                    attrList['EventStat'], \
                    attrList['EventTypeId'], \
                    attrList['FileName'],  \
                    attrList['FileTypeId'], \
                    attrList['GotReplica'], \
                    attrList['Guid'],  \
                    attrList['JobId'], \
                    attrList['MD5Sum'], \
                    attrList['FileSize'] ] ) 
      return result
      
  #############################################################################
  def updateReplicaRow(self, fileID, replica): #, name, location):
    result = self.db_.executeStoredProcedure('BKK_ORACLE.updateReplicaRow',[int(fileID), replica],False)
    return result
  
  #############################################################################
  def deleteJob(self, jobID):
    result = self.db_.executeStoredProcedure('BKK_ORACLE.deleteJob',[jobID], False)
    return result
  
  #############################################################################
  def deleteInputFiles(self, jobid):
    result = self.db_.executeStoredProcedure('BKK_ORACLE.deleteInputFiles',[jobid], False)
    return result
  
  #############################################################################
  def deleteFile(self, file):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def modifyReplica(self, fileID , name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertSimConditions(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.db_.executeStoredFunctions('BKK_ORACLE.insertSimConditions', LongType, [BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity])
  
  #############################################################################
  def getSimulationCondIdByDesc(self, desc):
    return self.db_.executeStoredFunctions('BKK_ORACLE.getSimulationCondIdByDesc', LongType, [desc])

  #############################################################################
  def getDataTakingCondId(self, condition):
    command = 'select count(*) from data_taking_conditions where ' 
    for param in condition:
      command +=  str(param)+'=\''+condition[param]+'\' and '
    command = command[:-4]
    res = self.db_._query(command)
    return res
  
  #############################################################################
  def insertDataTakingCond(self, conditions): 
    datataking = {  'BeamCond':None, \
                    'BeamEnergy':None, \
                    'MagneticField':None, \
                    'VELO':None, \
                    'IT':None,  \
                    'TT':None, \
                    'OT':None, \
                    'RICH1':None,  \
                    'RICH2':None, \
                    'SPD_PRS':None,\
                    'ECAL':None, \
                    'HCAL':None, \
                    'MUON':None, \
                    'L0':None, \
                    'HLT':None}
        
    for param in conditions:
      if not datataking.__contains__(param):
        gLogger.error("insert datataking error: "," the files table not contains "+param+" this attributte!!")
        return S_ERROR(" The datatakingconditions table not contains "+param+" this attributte!!")
      datataking[param] = conditions[param]
        
    res = self.db_.executeStoredFunctions('BKK_ORACLE.insertDataTakingCond', LongType, [datataking['BeamCond'], datataking['BeamEnergy'], \
                                                                                  datataking['MagneticField'], datataking['VELO'], \
                                                                                  datataking['IT'], datataking['TT'], datataking['OT'], \
                                                                                  datataking['RICH1'], datataking['RICH2'], \
                                                                                  datataking['SPD_PRS'], datataking['ECAL'], \
                                                                                  datataking['HCAL'], datataking['MUON'], datataking['L0'], datataking['HLT'] ])
    return res
  
  #############################################################################
  def removeReplica(self, fileName):
    result = self.checkfile(fileName) 
    if result['OK']:
      fileID = long(result['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'No')
      if res['OK']:
        return S_OK("Replica has ben removed!!!")
      else:
        return S_ERROR(res['Message'])      
    else:
      return S_ERROR('The file '+fileName+'not exist in the BKK database!!!')
  
  #############################################################################
  def getFileMetadata(self, lfns):
    result = {}
    for file in lfns:
      res = self.db_.executeStoredProcedure('BKK_ORACLE.getFileMetaData',[file])
      if not res['OK']:
        result[file]= res['Message']
      else:
        records = res['Value']  
        for record in records:
          row = {'ADLER32':record[1],'CreationDate':record[2],'EventStat':record[3],'EventTypeId':record[4],'FileType':record[5],'GotReplica':record[6],'GUID':record[7],'MD5SUM':record[8],'FileSize':record[9]}
          result[file]= row
    return S_OK(result)
  
  #############################################################################
  def exists(self, lfns):
    result ={}
    for file in lfns:
      res = self.db_.executeStoredFunctions('BKK_ORACLE.fileExists', LongType, [file])
      if not res['OK']:
        return S_ERROR(res['Message'])
      if res['Value'] ==0:
        result[file] = False
      else:
        result[file] = True
    return S_OK(result)
   
  #############################################################################
  def addReplica(self, fileName):
    result = self.checkfile(fileName) 
    if result['OK']:
      fileID = long(result['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'Yes')
      if res['OK']:
        return S_OK("Replica has ben added!!!")
      else:
        return S_ERROR(res['Message'])      
    else:
      return S_ERROR('The file '+fileName+'not exist in the BKK database!!!')

  #############################################################################
  def insertEventTypes(self, evid, desc, primary):
    return self.db_.executeStoredProcedure('BKK_ORACLE.insertEventTypes',[desc, evid, primary], False)
  
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.db_.executeStoredProcedure('BKK_ORACLE.updateEventTypes',[desc, evid, primary], False)
  
  #############################################################################
  #
  #          MONITORING
  #############################################################################
  def getJobsNb(self, prodid):
    return self.db_.executeStoredProcedure('BKK_MONITORING.getJobsNb', [prodid])
  
  #############################################################################
  def getNumberOfEvents(self, prodid):
    return self.db_.executeStoredProcedure('BKK_MONITORING.getNumberOfEvents', [prodid])
  
  #############################################################################
  def getSizeOfFiles(self, prodid):
    return self.db_.executeStoredProcedure('BKK_MONITORING.getSizeOfFiles', [prodid])
  
  #############################################################################
  def getNbOfFiles(self, prodid):
    return self.db_.executeStoredProcedure('BKK_MONITORING.getNbOfFiles', [prodid])
  
  #############################################################################
  def getProductionInformation(self, prodid):
    return self.db_.executeStoredProcedure('BKK_MONITORING.getProductionInformation', [prodid])
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    return self.db_.executeStoredProcedure('BKK_MONITORING.getJobsbySites', [prodid])
    
  