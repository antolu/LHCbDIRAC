########################################################################
# $Id$
########################################################################

"""

"""

from xml.dom.minidom                                                                    import parse, parseString
from LHCbDIRAC.NewBookkeepingSystem.Service.XMLReader.JobReader                            import JobReader
from LHCbDIRAC.NewBookkeepingSystem.Service.XMLReader.ReplicaReader                        import ReplicaReader
from DIRAC.ConfigurationSystem.Client.Config                                            import gConfig
from DIRAC                                                                              import gLogger, S_OK, S_ERROR
from LHCbDIRAC.NewBookkeepingSystem.DB.BookkeepingDatabaseClient                           import BookkeepingDatabaseClient
#from DIRAC.NewBookkeepingSystem.Client.BookkeepingClient                                  import BookkeepingClient
from DIRAC.DataManagementSystem.Client.ReplicaManager                                   import ReplicaManager
from LHCbDIRAC.NewBookkeepingSystem.Service.XMLReader.Job.FileParam                        import FileParam
from LHCbDIRAC.NewBookkeepingSystem.Service.XMLReader.Job.JobParameters                    import JobParameters
from LHCbDIRAC.NewBookkeepingSystem.DB.DataTakingConditionInterpreter                      import *
import os,sys,datetime,re

__RCSID__ = "$Id$"

global dataManager_
dataManager_ = BookkeepingDatabaseClient()
#dataManager_ = BookkeepingClient()

class XMLFilesReaderManager:

  #############################################################################
  def __init__(self):

    self.jobReader_ = JobReader()
    self.replicaReader_ = ReplicaReader()

    #self.dataManager_ = BookkeepingDatabaseClient()
    self.replicaManager_ = ReplicaManager()

  #############################################################################
  def readFile(self, filename):
    """

    """
    try:
      stream = open(filename)
      doc = parse(stream)
      stream.close()


      docType = doc.doctype #job or replica
      type = docType.name.encode('ascii')
    except Exception,ex:
      gLogger.error("XML reading error",filename)
      return S_ERROR(ex)

    return type,doc,filename

  #############################################################################
  def readXMLfromString(self, name, xmlString):

    try:
      doc = parseString(xmlString)

      docType = doc.doctype #job or replica
      type = docType.name.encode('ascii')

      if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, "IN Memory")
        result = self.processReplicas(replica)
        del replica
        return result
      else:
        if type == 'Job':
          job = self.jobReader_.readJob(doc, "IN Memory")
          result = self.processJob(job)
          del job
          return result
        else:
          gLogger.error("unknown XML file!!!")
    except Exception,ex:
      gLogger.error("XML reading error",ex)
      return S_ERROR(ex)


  #############################################################################
  def processJob(self, job, errorReport=False):
    """

    """
    gLogger.info("Start Processing")

    deleteFileName = job.getFileName()

    ##checking
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      name = file.getFileName()
      result = dataManager_.checkfile(name)
      if result['OK']:
        fileID = long(result['Value'][0][0])
        file.setFileID(fileID)
      else:
        return S_ERROR('The file '+name+' not exist in the BKK database!!')

    outputFiles = job.getJobOutputFiles()
    dqvalue = None
    for file in outputFiles:
      name = file.getFileName()
      result = dataManager_.checkfile(name)
      if result['OK']:
        return S_OK("The file " + str(name) + " already exists.")

      typeName = file.getFileType()
      typeVersion = file.getFileVersion()
      result = dataManager_.checkFileTypeAndVersion(typeName, typeVersion)
      if not result['OK']:
        return S_ERROR("The type " + str(typeName) + ", version " + str(typeVersion) +"is missing.\n")
      else:
        typeID = long(result['Value'][0][0])
        file.setTypeID(typeID)

      params = file.getFileParams()
      evtExists = False
      runExists = False
      for param in params:
        paramName = param.getParamName()

        if paramName == "EventStat":
          if param.getParamValue()=='' and file.getFileType() in ['BRUNELHIST','DAVINCIHIST','GAUSSHIST','BOOLEHIST']:
            param.setParamValue(None) # default value
          elif param.getParamValue()=='':
            return S_ERROR('EventStat value is null')
          else:
            eventNb = long(param.getParamValue())
            if eventNb < 0:
              return S_ERROR("The event number not greater 0!")

        if paramName == "FullStat":
          fullStat = long(param.getParamValue())
          if fullStat <= 0:
            return S_ERROR("The fullStat not greater 0!")

        if paramName == "EventType":
          value = long(param.getParamValue())
          result = dataManager_.checkEventType(value)
          if not result['OK']:
            return S_ERROR("The event type " + str(value) + " is missing.\n")
        
        gLogger.debug('EventTypeId checking!')
        if paramName == "EventTypeId":
          if param.getParamValue() != '':
            value = long(param.getParamValue())
            result = dataManager_.checkEventType(value)
            if not result['OK']:
              return S_ERROR("The event type " + str(value) + " is missing.\n")
            evtExists = True              
      
      if not evtExists and file.getFileType() not in ['LOG']:
        inputFiles = job.getJobInputFiles()
        if len(inputFiles) > 0:
          fileName = inputFiles[0].getFileName()
          res = dataManager_.getFileMetadata([fileName])
          if res['OK']:
            value = res['Value']
            if value[fileName].has_key('EventTypeId'):
              if file.exists('EventTypeId'):
                param = file.getParam('EventTypeId')
                param.setParamValue(str(value[fileName]['EventTypeId']))
              else:
                newFileParams = FileParam()
                newFileParams.setParamName('EventTypeId')
                newFileParams.setParamValue(str(value[fileName]['EventTypeId']))
                file.addFileParam(newFileParams)
          else:
            return S_ERROR(res['Message']) 
        elif job.getOutputFileParam('EventTypeId') != None:
          param = job.getOutputFileParam('EventTypeId')
          newFileParams = FileParam()
          newFileParams.setParamName('EventTypeId')
          newFileParams.setParamValue(param.getParamValue())
          file.addFileParam(newFileParams)
        else:
           return S_ERROR('I can not fill the EventTypeId because there is no input files!')
      
      infiles = job.getJobInputFiles()
      if not job.exists('RunNumber') and len(infiles) > 0:
        fileName = infiles[0].getFileName()
        retVal = dataManager_.getRunNumber(fileName)
        
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        runnumber = retVal['Value']
        
        if runnumber != None: 
          newJobParams = JobParameters()
          newJobParams.setName('RunNumber')
          newJobParams.setValue(str(runnumber))
          job.addJobParams(newJobParams)
          prod = job.getParam('Production').getValue()
          
          retVal = dataManager_.getProductionProcessingPass(prod)
          if retVal['OK']:
            proc = retVal['Value']
          
            retVal = dataManager_.getRunFlag(runnumber, proc)
            if retVal['OK']:
              dqvalue = retVal['Value']
            else:
              dqvalue = None
              gLogger.error('The data quality working group did not checked the run!!')
          else:
            dqvalue = None
            gLogger.error('Bkk can not set the quality flag because the processing pass is missing!')
      
    inputfiles = job.getJobInputFiles()
    sumEventInputStat = 0
    sumEvtStat = 0
    sumLuminosity = 0
    
    for i in inputfiles:
      fname = i.getFileName()
      res = dataManager_.getJobInfo(fname)
      
      if res['OK']:
        value = res["Value"]
        if len(value) > 0 and value[0][2] != None:
          sumEventInputStat += value[0][2]
      else:
        return S_ERROR(res['Message'])
      res = dataManager_.getFileMetadata([fname])
      if res['OK']:
        value = res['Value']
        if value[fname]['EventStat'] != None:
          sumEvtStat += value[fname]['EventStat']
        if value[fname]['Luminosity'] != None:
          sumLuminosity += value[fname]['Luminosity']
      
    evtinput = 0
    if long(sumEvtStat) > long(sumEventInputStat):
      evtinput = sumEvtStat
    else:
      evtinput = sumEventInputStat
        
    currentEventInputStat = job.getParam('EventInputStat')
    if currentEventInputStat != None:
        currentEventInputStat.setValue(evtinput)
    elif not job.exists('EventInputStat') and len(inputfiles) > 0:
      newJobParams = JobParameters()
      newJobParams.setName('EventInputStat')
      newJobParams.setValue(str(evtinput))
      job.addJobParams(newJobParams)
    
    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      if file.getFileType() not in ['LOG'] and sumLuminosity > 0 and not file.exists('Luminosity'):
        newFileParams = FileParam()
        newFileParams.setParamName('Luminosity')
        newFileParams.setParamValue(sumLuminosity)
        file.addFileParam(newFileParams)
              
      ################

    config = job.getJobConfiguration()
    params = job.getJobParams()

    for param in params:
      if param.getName() == "RunNumber":
        value = long(param.getValue())
        if value <= 0:
          return S_ERROR('The run number not greater 0!')

    if  dqvalue == None and job.exists('JobType'):  
      jobtype = job.getParam('JobType')
      jvalue = jobtype.getValue() 
      if jvalue != '' and re.search('MERGE',jvalue.upper()):
        inputfiles = job.getJobInputFiles()
        if len(inputfiles) > 0:
          fileName = inputfiles[0].getFileName()
          res = dataManager_.getFileMetadata([fileName])
          if res['OK']:
            value = res['Value']
            if value[fileName].has_key('DQFlag'):
              dqvalue = value[fileName]['DQFlag']
          else:
            gLogger.warn(res['Message'])
      job.removeParam('JobType')        
    
    if job.exists('JobType'):  
      job.removeParam('JobType')

    result = self.__insertJob(job)

    if not result['OK']:
      return S_ERROR("Unable to create Job : " + str(config.getConfigName()) + ", " + str(config.getConfigVersion()) + ", " + str(config.getDate()) + ".\n"+'Error: '+str(result['Message']))
    else:
      jobID = long(result['Value'])
      job.setJobId(jobID)
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      result = dataManager_.insertInputFile(job.getJobId(), file.getFileID())
      if not result['OK']:
        dataManager_.deleteJob(job.getJobId())
        return S_ERROR("Unable to add " + str(file.getFileName()))


    outputFiles = job.getJobOutputFiles()
    prod = job.getParam('Production').getValue()
    retVal = dataManager_.getProductionOutputFiles(prod)
    if not retVal['OK']:
      return retVal
    outputFileTypes = retVal['Value']
    for file in outputFiles:
      if dqvalue != None:
        newFileParams = FileParam()
        newFileParams.setParamName('QualityId')
        newFileParams.setParamValue(dqvalue)
        file.addFileParam(newFileParams)
      if not job.exists('RunNumber'): #if it is MC
        newFileParams = FileParam()
        newFileParams.setParamName('QualityId')
        newFileParams.setParamValue('OK')
        file.addFileParam(newFileParams)
      ftype = file.getFileType()
      if outputFileTypes.has_key(ftype):
        vFileParams = FileParam()
        vFileParams.setParamName('VisibilityFlag')
        vFileParams.setParamValue(outputFileTypes[ftype])
        file.addFileParam(vFileParams)
        
      result = self.__insertOutputFiles(job, file)
      if not result['OK']:
        dataManager_.deleteInputFiles(job.getJobId())
        dataManager_.deleteJob(job.getJobId())
        return S_ERROR("Unable to create file " + str(file.getFileName()) + "!"+"ERROR: "+result["Message"])
      else:
        id = long(result['Value'])
        file.setFileID(id)

      qualities = file.getQualities()
      for quality in qualities:
        group = quality.getGroup()
        flag = quality.getFlag()
        result = dataManager_.insertQuality(file.getFileID(), group, flag)
        if not result['OK']:
          return S_ERROR("Unable to create Quality " + str(group) + "/" + str(flag) + "\" for file " + str(file.getFileName()) + ".\n")
        else:
          qualityID = long(result['Value'])
          quality.setQualityID(qualityID)

        params = quality.getParams()
        for param in params:
           name = param.getName()
           value = param.getValue()
           result = dataManager_.insertQualityParam(file.getFileID(), quality.getQualityID(), name, value)
           if not result['OK']:
             return S_ERROR("Unable to create Parameter \"" + str(name) + " = " + str(value) + "\" for quality " + group + "/" + flag + "\".\n")

      replicas = file.getReplicas()
      for replica in replicas:
        params = replica.getaprams()
        for param in params: # just one param exist in params list, because JobReader only one param add to Replica
          name = param.getName()
          location = param.getLocation()
        result = dataManager_.updateReplicaRow(file.getFileID(), 'No')  #, name, location)
        if not result['OK']:
          return S_ERROR("Unable to create Replica " + str(name) +".\n")

    gLogger.info("End Processing!" )

    return S_OK()

  def __insertJob(self, job):

    config = job.getJobConfiguration()

    jobsimcondtitions = job.getSimulationCond()
    simulations = {}
    production = None

    condParams = job.getDataTakingCond()
    if condParams != None:
      datataking = condParams.getParams()  
      config = job.getJobConfiguration()
      
      ver = config.getConfigVersion() # online bug fix
      ver = ver.capitalize()
      config.setConfigVersion(ver)
            
      context = Context(datataking, config.getConfigName())
      conditions = [BeamEnergyCondition(),VeloCondition(), MagneticFieldCondition(), EcalCondition(), HcalCondition(), HltCondition(), ItCondition(), LoCondition(), \
              MuonCondition(), OtCondition(), Rich1Condition(), Rich2Condition(), Spd_prsCondition(), TtCondition(), VeloPosition()]
      for condition in conditions:
        condition.interpret(context)
      
      datataking['Description'] = context.getOutput()
        
      res = dataManager_.getDataTakingCondDesc(datataking)
      dataTackingPeriodID = None
      if res['OK']:
        daqid = res['Value']
        if len(daqid)!=0: #exist in the database datataking
          dataTackingPeriodID = res['Value'][0][0]
          gLogger.debug('Data taking condition id', dataTackingPeriodID)
        else:
          res = dataManager_.insertDataTakingCond(datataking)
          if not res['OK']:
            return S_ERROR("DATA TAKING Problem"+str(res['Message']))
          else:
            dataTackingPeriodID = res['Value']
      else:
        return S_ERROR("DATA TAKING Problem"+str(res['Message']))

      #insert processing pass
      programName = None
      programVersion = None
      found = False
      for param in job.getJobParams():
        if param.getName() =='ProgramName':
          programName = param.getValue()
        elif param.getName() =='ProgramVersion':
          programVersion = param.getValue()
        elif param.getName() == 'RunNumber':
          production = long(param.getValue()) * -1
          found = True
      
      if not found:
        gLogger.error('Runn number is missing!')
        return S_ERROR('Runn number is missing!')
      
      retVal = dataManager_.getStepIdandNameForRUN(programName, programVersion)
      
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      steps = {'Steps':[{'StepId':retVal['Value'][0],'StepName':retVal['Value'][1],'Visible':'Y'}]}
      gLogger.debug('Pass_indexid', steps)
      gLogger.debug('Data taking', dataTackingPeriodID)
      gLogger.debug('production', production)
      
      res = dataManager_.addProduction(production, simcond=None, daq=dataTackingPeriodID, steps=steps['Steps'], inputproc='')
      
      if res['OK']:
        gLogger.info("New processing pass has been created!")
        gLogger.info("New production is:",production)
      else:
        gLogger.error('Unable to create processing pass!',res['Message'])
        return S_ERROR('Unable to create processing pass!')
      

    attrList = {'ConfigName':config.getConfigName(), \
                 'ConfigVersion':config.getConfigVersion(), \
                 'JobStart':None}

    for param in job.getJobParams():
      attrList[str(param.getName())] = param.getValue()

    res = dataManager_.checkProcessingPassAndSimCond(attrList['Production'])
    if not res['OK']:
      gLogger.error('check processing pass and simulation condition error',res['Message'] )
    else:
      value = res['Value']
      if value[0][0]==0:
        gLogger.warn('Missing processing pass and simulation conditions!(Please fill it!) Production=',str(attrList['Production']))


    if attrList['JobStart']==None:
      #date = config.getDate().split('-')
      #time = config.getTime().split(':')
      #dateAndTime = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
      attrList['JobStart']=config.getDate()+' '+config.getTime()

    if production != None: # for the online registration
      attrList['Production'] = production

    res = dataManager_.insertJob(attrList)
    return res

  #############################################################################
  def __insertOutputFiles(self, job, file):

    attrList = {  'FileName':file.getFileName(),  \
                  'FileTypeId':file.getTypeID(), \
                  'JobId':job.getJobId()}


    fileParams = file.getFileParams()
    for param in fileParams:
      attrList[str(param.getParamName())] = param.getParamValue()
    res = dataManager_.insertOutputFile(attrList)
    return res

  #############################################################################
  def processReplicas(self, replica, errorReport=False):
    """

    """
    file = replica.getFileName()
    gLogger.info("Processing replicas: " + str(file))
    fileID = -1

    params = replica.getaprams()
    delete = True

    replicaFileName = ""
    for param in params:
      name = param.getName()
      replicaFileName = param.getFile()
      location = param.getLocation()
      delete = param.getAction() == "Delete"

      result = dataManager_.checkfile(replicaFileName)

      if not result['OK']:
        message = "No replica can be ";
        if (delete):
           message += "removed"
        else:
           message += "added"
        message += " to file " + str(file) + " for " + str(location) + ".\n"
        return S_ERROR(message)
      else:
        fileID = long(result['Value'][0][0])
        gLogger.info(fileID)

      if (delete):
        result = self.replicaManager_.getReplicas(replicaFileName)
        list = result['Value']
        replicaList = list['Successful']
        if len(replicaList) == 0:
          result = dataManager_.updateReplicaRow(fileID,"No")
          if not result['OK']:
            gLogger.warn("Unable to set the Got_Replica flag for " + str(replicaFileName))
            return S_ERROR("Unable to set the Got_Replica flag for " + str(replicaFileName))

      else:
        result = dataManager_.updateReplicaRow(fileID, "Yes")
        if not result['OK']:
          return S_ERROR("Unable to set the Got_Replica flag for " + str(replicaFileName))


    gLogger.info("End Processing replicas!")

    return S_OK()

  