########################################################################
# $Id$
########################################################################

"""

"""

from xml.dom.minidom                                                              import parse, parseString
from LHCbDIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient                    import FileSystemClient
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.JobReader                            import JobReader
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.ReplicaReader                        import ReplicaReader
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC                                                                        import gLogger, S_OK, S_ERROR
from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
#from DIRAC.BookkeepingSystem.Client.BookkeepingClient                             import BookkeepingClient
from DIRAC.DataManagementSystem.Client.ReplicaManager                             import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Agent.ErrorReporterMgmt.ErrorReporterMgmt            import ErrorReporterMgmt
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.FileParam                        import FileParam
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.JobParameters                    import JobParameters
from LHCbDIRAC.BookkeepingSystem.DB.DataTakingConditionInterpreter                import *
import os,sys,datetime

__RCSID__ = "$Id$"

global dataManager_
dataManager_ = BookkeepingDatabaseClient()
#dataManager_ = BookkeepingClient()

class XMLFilesReaderManager:

  #############################################################################
  def __init__(self):
    self.fileClient_ = FileSystemClient()
    self.jobReader_ = JobReader()
    self.replicaReader_ = ReplicaReader()

    self.baseDir_ = gConfig.getValue("XMLProcessing", "/opt/bookkeeping/XMLProcessing/")
    self.errorsTmp_ = self.baseDir_ + "ErrorsTmp/"
    self.jobs_ = []
    self.replicas_ = []
    #self.dataManager_ = BookkeepingDatabaseClient()
    self.replicaManager_ = ReplicaManager()
    self.errorMgmt_ = ErrorReporterMgmt()

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
        self.errorMgmt_.reportError (10, "The file " + name + " is missing.", deleteFileName, errorReport)
        return S_ERROR('The file '+name+' not exist in the BKK database!!')

    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      name = file.getFileName()
      result = dataManager_.checkfile(name)
      if result['OK']:
        self.errorMgmt_.reportError (11, "The file " + str(name) + " already exists.", deleteFileName, errorReport)
        return S_OK("The file " + str(name) + " already exists.")

      typeName = file.getFileType()
      typeVersion = file.getFileVersion()
      result = dataManager_.checkFileTypeAndVersion(typeName, typeVersion)
      if not result['OK']:
        self.errorMgmt_.reportError (12, "The type " + str(typeName) + ", version " + str(typeVersion) +"is missing.\n", deleteFileName, errorReport)
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
              self.errorMgmt_.reportError (13,"The event number not greater 0!", deleteFileName, errorReport)
              return S_ERROR("The event number not greater 0!")

        if paramName == "FullStat":
          fullStat = long(param.getParamValue())
          if fullStat <= 0:
            self.errorMgmt_.reportError (13,"The fullStat not greater 0!", deleteFileName, errorReport)
            return S_ERROR("The fullStat not greater 0!")

        if paramName == "EventType":
          value = long(param.getParamValue())
          result = dataManager_.checkEventType(value)
          if not result['OK']:
            self.errorMgmt_.reportError (13, "The event type " + str(value) + " is missing.\n", deleteFileName, errorReport)
            return S_ERROR("The event type " + str(value) + " is missing.\n")
        
        gLogger.debug('EventTypeId checking!')
        if paramName == "EventTypeId":
          if param.getParamValue() != '':
            value = long(param.getParamValue())
            result = dataManager_.checkEventType(value)
            if not result['OK']:
              self.errorMgmt_.reportError (13, "The event type " + str(value) + " is missing.\n", deleteFileName, errorReport)
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
        value = retVal['Value']
        if len(value) > 0 and value[0][0] != None: 
          runnumber = value[0][0]
          newJobParams = JobParameters()
          newJobParams.setName('RunNumber')
          newJobParams.setValue(str(runnumber))
          job.addJobParams(newJobParams)
      
    inputfiles = job.getJobInputFiles()
    sumEventInputStat = 0
    for i in inputfiles:
      fname = i.getFileName()
      res = dataManager_.getJobInfo(fname)
      if res['OK']:
        value = res["Value"]
        if len(value) > 0 and value[0][2] != None:
          sumEventInputStat += value[0][2]
      else:
        return S_ERROR(res['Message'])
    
      
    currentEventInputStat = job.getParam('EventInputStat')
    if currentEventInputStat != None:
      if currentEventInputStat.getValue() != None: 
        if currentEventInputStat.getValue() != '':
          if long(sumEventInputStat) > long(currentEventInputStat.getValue()):
            currentEventInputStat.setValue(sumEventInputStat)
    
    if not job.exists('EventInputStat') and len(inputfiles) > 0:
      newJobParams = JobParameters()
      newJobParams.setName('EventInputStat')
      newJobParams.setValue(str(sumEventInputStat))
      job.addJobParams(newJobParams)
  
      ################

    config = job.getJobConfiguration()
    params = job.getJobParams()

    for param in params:
      if param.getName() == "RunNumber":
        value = long(param.getValue())
        if value <= 0:
          self.errorMgmt_.reportError (13, "The run number not greater 0!" , deleteFileName, errorReport)
          return S_ERROR('The run number not greater 0!')


    result = self.__insertJob(job)

    if not result['OK']:
      self.errorMgmt_.reportError (13, "Unable to create Job : " + str(config.getConfigName()) + ", " + str(config.getConfigVersion()) + ", " + str(config.getDate()) + ".\n", deleteFileName, errorReport)
      return S_ERROR("Unable to create Job : " + str(config.getConfigName()) + ", " + str(config.getConfigVersion()) + ", " + str(config.getDate()) + ".\n"+'Error: '+str(result['Message']))
    else:
      jobID = long(result['Value'])
      job.setJobId(jobID)
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      result = dataManager_.insertInputFile(job.getJobId(), file.getFileID())
      if not result['OK']:
        dataManager_.deleteJob(job.getJobId())
        self.errorMgmt_.reportError (16, "Unable to add " + str(file.getFileName()), deleteFileName, errorReport)
        return S_ERROR("Unable to add " + str(file.getFileName()))


    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      result = self.__insertOutputFiles(job, file)
      if not result['OK']:
        dataManager_.deleteInputFiles(job.getJobId())
        dataManager_.deleteJob(job.getJobId())
        self.errorMgmt_.reportError (17, "Unable to create file " + str(file.getFileName()) + "!", deleteFileName, errorReport)
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
          self.errorMgmt_.reportError(19, "Unable to create Quality " + str(group) + "/" + str(flag) + "\" for file " + str(file.getFileName()) + ".\n", deleteFileName, errorReport)
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
             self.errorMgmt_.reportError (20, "Unable to create Parameter \"" + str(name) + " = " + str(value) + "\" for quality " + group + "/" + flag + "\".\n", deleteFileName, errorReport)
             return S_ERROR("Unable to create Parameter \"" + str(name) + " = " + str(value) + "\" for quality " + group + "/" + flag + "\".\n")

      replicas = file.getReplicas()
      for replica in replicas:
        params = replica.getaprams()
        for param in params: # just one param exist in params list, because JobReader only one param add to Replica
          name = param.getName()
          location = param.getLocation()
        result = dataManager_.updateReplicaRow(file.getFileID(), 'No')  #, name, location)
        if not result['OK']:
          self.errorMgmt_.reportError(21, "Unable to create Replica " + str(name) +".\n", deleteFileName, errorReport)
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
        
      res = dataManager_.getDataTakingCondId(datataking)
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
      retVal = dataManager_.getPassIndexID(programName, programVersion)
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      passIndex = retVal['Value']
      gLogger.debug('Pass_indexid', passIndex)
      gLogger.debug('Data taking', dataTackingPeriodID)
      gLogger.debug('production', production)
      res = dataManager_.insertProcessing_pass(production, passIndex, dataTackingPeriodID)
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
  def __checkProgramNameIsGaussTMP(self, job):
    '''
    temporary method I will remove it
    '''
    gLogger.info('__checkProgramNameIsGaussTMP','Job is Gauss Job?')
    for param in job.getJobParams():
      if param.getName()=='ProgramName':
        value=param.getValue()
        if value.upper()=='GAUSS':
          return True
        else:
          return False

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
        self.errorMgmt_.reportError(23, message, file, errorReport)
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
            self.errorMgmt_.reportError(26, "Unable to set the Got_Replica flag for " + str(replicaFileName), file, errorReport)
            return S_ERROR("Unable to set the Got_Replica flag for " + str(replicaFileName))

      else:
        result = dataManager_.updateReplicaRow(fileID, "Yes")
        if not result['OK']:
          self.errorMgmt_.reportError(26, "Unable to set the Got_Replica flag for " + str(replicaFileName), file, errorReport)
          return S_ERROR("Unable to set the Got_Replica flag for " + str(replicaFileName))


    gLogger.info("End Processing replicas!")

    return S_OK()

  #############################################################################
  def getJobs(self):
    return self.jobs_

  #############################################################################
  def getReplicas(self):
    return self.replicas_

  #############################################################################
  def _listTodoDirectory(self):
    return self.fileClient_.list(self.baseDir_+"ToDo")
  #############################################################################
  def fileaction(self, fn):
    """

    """
    try:
      fptr = open(fn,'r')
      data = fptr.read()
      fptr.close()
      if ( data.find('<Error id="10">')>0 ):
      # File does not (yet) exist
        return 'recover', 10
      elif ( data.find('<Error id="23">')>0 ):
      # No replica can be added to file
        return 'recover', 23
      elif ( data.find('<Error id="26">')>0 ):
        # Error while connectiong to genCatalog server
        return 'recover', 26
      elif ( data.find('<Error id="12">')>0 ):
      # The type LOG, version 0is missing. Io exception: Connection reset by peer: socket write error
        return 'recover', 12
      elif ( data.find('<Error id="7">')>0 ):
      # Attribute "SE" must be declared for element type "Replica".
        return 'save', 7
      elif ( data.find('<Error id="2">')>0 ):
      # Open quote is expected for attribute "Name|".
        return 'save', 2
      elif ( data.find('<Error id="25">')>0 ):
      # File already had a replica at
        return 'save', 25
      elif ( data.find('<Error id="11">')>0 ):
      # File already exists
        return 'save', 11
      else:
        #print 'Skip:', todo_dir+f
        return 'none', None
    except Exception, ex:
      gLogger.warn(ex)
      gLogger.info(fn+"file removed the ErrorTmp directory")
      name = os.path.split(fn[:-6])[1]
      self.fileClient_.rename(fn[:-6], self.errorsTmp_+name)
      self.fileClient_.rename(fn, self.errorsTmp_+name)
      #self.fileClient_.rm(fn)
      return 'none', None

  #############################################################################
  def _listErrorDirectory(self):
    """

    """
    fileList = self.fileClient_.list(self.baseDir_+"Errors")

    files = []
    for file in fileList:
      if file[-6:] != '.error':
        fn = file + ".error"
        action, err = self.fileaction(fn)
        if ( action == 'recover' ):
          files += [file]
        else:
          name = os.path.split(file)[1]
          self.fileClient_.rename(file, self.errorsTmp_ + name)
          gLogger.info(file+" file moved the ErrorTmp directory!!!")
          gLogger.info(fn+" file moved the ErrorTmp directory!!!")
          self.fileClient_.rename(fn, self.errorsTmp_ + name+".error")

        """
        very important to remove also the error messages, because we have to now
        what is the problem!!!!
        """
        #self.fileClient_.rm(fn)
    return files

  #############################################################################
  def initialize(self):
    """

    """
    jobList = self._listTodoDirectory()

    for fileName in jobList:
      type,doc,fullpath = self.readFile(fileName)
      if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, fullpath)
        self.replicas_ += [replica]
      else:
        if type == 'Job':
          try:
            job = self.jobReader_.readJob(doc, fullpath)
            self.jobs_ += [job]
          except Exception,ex:
            gLogger.error("XML reading error2",ex)
        else:
          name = os.path.split(fileName)[1]
          self.fileClient_.rename(fileName, self.errorsTmp_ + name)
          self.fileClient_.rm(fileName)
          gLogger.error("unknown XML file!!!")


    errorList = self._listErrorDirectory()
    for fileName in errorList:
      type,doc,fullpath = self.readFile(fileName)
      if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, fullpath)
        self.replicas_ += [replica]
      else:
        if type == 'Job':
          job = self.jobReader_.readJob(doc, fullpath)
          self.jobs_ += [job]
        else:
          name = os.path.split(fileName)[1]
          self.fileClient_.rename(fileName, self.errorsTmp_ + name)
          self.fileClient_.rm(fileName)
          print fileName,self.errorsTmp_ + name
          gLogger.error("unknown XML file!!!")

  #############################################################################
  def destroy(self):
    del self.jobs_[:]
    del self.replicas_[:]

  #############################################################################



