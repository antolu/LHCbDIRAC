########################################################################
# $Id: JobReader.py 20219 2010-01-22 10:27:30Z atsareg $
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.JobConfiguration           import JobConfiguration
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.JobOption                  import JobOption
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.File                       import File
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.InputFile                  import InputFile
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.JobParameters              import JobParameters
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.Job                        import Job
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.FileParam                  import FileParam
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.SimulationConditions       import SimulationConditions
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.DataTakingConditions       import DataTakingConditions
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Replica.FileReplica            import FileReplica
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Replica.ReplicaParam           import ReplicaParam
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.Quality                    import Quality
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.QualityParameters          import QualityParameters
from DIRAC                                                                  import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: JobReader.py 20219 2010-01-22 10:27:30Z atsareg $"


class JobReader:

  #############################################################################  
  def __init__(self):
    pass
  
  #############################################################################      
  def readJob(self, doc, fileName):
    """
    
    """
    job = Job()
    job.setFileName(fileName)
    gLogger.info("Reading job from" + str(fileName) + "XML!")
            
    self.__readJobConfigurations(doc, job)
    self.__readJobOptions(doc, job)
    self.__readJobTypeParameters(doc, job)
    self.__readJobInputFiles(doc, job)
    self.__readJobOutputFiles(doc, job)
    self.__readJobSimulationConditions(doc, job)
    self.__readJobDataTakingConditions(doc, job)
                    
    gLogger.info("Job reading fhinished succesfull!")
    return job
    
  #############################################################################
  def __readJobConfigurations(self, doc, job):
    """
    
    """
    
    conf = JobConfiguration()
    jobElements = doc.getElementsByTagName('Job')
    for node in jobElements:
    
        name = node.getAttributeNode('ConfigName')
        if name != None:
            conf.setConfigName(name.value.encode('ascii'))
        else:
            gLogger.error("<ConfigName> XML tag is missing!!")
        
        version = node.getAttributeNode('ConfigVersion')
        if version != None:
            conf.setConfigVersion(version.value.encode('ascii'))
        else:
            gLogger.error("<ConfigVersion> XML tag is missing!!")
        
        date = node.getAttributeNode('Date')
        if date != None:
            conf.setDate(date.value.encode('ascii'))
        else:
            gLogger.error("<Date> XML tag is missing!!")
        
        time = node.getAttributeNode('Time') 
        if time != None:
            conf.setTime(time.value.encode('ascii'))
        else:
            gLogger.error("<Time> XML tag is missing!!")                
    
    job.setJobConfiguration(conf)
    
  #############################################################################
  def __readJobOptions(self, doc, job):
    """
    
    """
    jobOptions = doc.getElementsByTagName('JobOption')
    for node in jobOptions:
        options = JobOption()
        recipient = node.getAttributeNode('Recipient')
        if recipient != None:
            options.setRecipient(recipient.value.encode('ascii'))
        else:
            gLogger.warn("<Recipinet> jobOption XML tag is missing!!")
        
        name = node.getAttributeNode('Name')
        if name != None:
            options.setName(name.value.encode('ascii'))
        else:
          gLogger.warn("<Name> JobOption XML tag is missing!!")
        
        value = node.getAttributeNode('Value')
        if value != None:
          options.setValue(value.value.encode('ascii'))
        else:
          gLogger.warn("<Value> JobOption XML tag is missing!!")
        
        job.addJobOptions(options)
  
  #############################################################################
  def __readJobTypeParameters(self, doc, job):
    """
    
    """
    
    jobTypeParameters = doc.getElementsByTagName('TypedParameter')
    
    for node in jobTypeParameters:
        parameters = JobParameters()
        name =  node.getAttributeNode('Name')
        if name != None:
          parameters.setName(name.value.encode('ascii'))
        else:
          gLogger.warn("<Name> TypedParameter XML tag is missing!!")
        
        value = node.getAttributeNode('Value')
        if value != None:
          parameters.setValue(value.value.encode('ascii'))
        else:
          gLogger.warn("<Value> TypedParameter XML tag is missing!!")
          
        type = node.getAttributeNode('Type')
        if type != None:
          parameters.setType(type.value.encode('ascii'))
        else:
          gLogger.warn("<Type> TypedParameter XML tag is missing!!")
          
        job.addJobParams(parameters)      
    
  ########################################################################
  def __readJobInputFiles(self, doc, job):
    
    jobInputFile = doc.getElementsByTagName('InputFile')
    for node in jobInputFile:
        inputFile = InputFile()
        input = node.getAttributeNode('Name')
        if input != None:
          inputFile.setFileName(input.value.encode('ascii'))   
        else:
          gLogger.error("<Name> InputFile XML tag is missing!!")
          
        job.addJobInputFiles(inputFile)
  
  ########################################################################
  def __readJobOutputFiles(self, doc, job):
    """
    
    """
    jobOutputFiles = doc.getElementsByTagName('OutputFile')
    for node in jobOutputFiles:
        outputFile = File()
        
        name = node.getAttributeNode('Name')
        if name != None:
          outputFile.setFileName(name.value.encode('ascii'))
        else:
          gLogger.error("<Name> Outputfile XML tag is missing!!")
        
        type = node.getAttributeNode('TypeName')
        if type != None:
          outputFile.setFileType(type.value.encode('ascii'))
        else:
          gLogger.error("<Type> outputfile XML tag is missing!!")
        
        version = node.getAttributeNode('TypeVersion')
        if version != None:
             outputFile.setFileVersion(version.value.encode('ascii'))
        else:
          gLogger.error("<Version> outputfile XML tag is missing!!")

        fileparams = node.getElementsByTagName('Parameter')
        for param in fileparams:
            outputFileParams = FileParam()
            
            name = param.getAttributeNode('Name')
            if name != None:
              outputFileParams.setParamName(name.value.encode('ascii'))
            else:
              gLogger.error("<Name> outputfile Parameter XML tag is missing!!")
            
            value = param.getAttributeNode('Value')
            if value != None:          
              outputFileParams.setParamValue(value.value.encode('ascii'))
            else:
              gLogger.error("<Value> outputfile Parameter XML tag is missing!!")
            
            outputFile.addFileParam(outputFileParams)
        
        replicas = doc.getElementsByTagName('Replica') ## I have to check doc ? no node? !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        
        for replica in replicas:
          rep = FileReplica()
          param = ReplicaParam()
          name = replica.getAttributeNode("Name")
          location = replica.getAttributeNode("Location")
          if name != None:
            param.setName(name.value.encode('ascii'))
          if location != None:
            param.setLocation(location.value.encode('ascii'))
            rep.addParam(param)
          outputFile.addReplicas(rep)
          
        qualities = node.getElementsByTagName("Quality")
  
        for quality in qualities:
          fileQuality = Quality()
          group = quality.getAttributeNode("Group")
          if group != None:
            fileQuality.setGroup(group.value.encode('ascii'))
          else:
            gLogger.warn("<Group> Quality XML tag is missing!!")
          
          flag = quality.getAttributeNode("Flag")
          if flag != None:
            fileQuality.setFlag(flag.value.encode('ascii'))
          else:
            gLogger.warn("<Flag> Quality XML tag is missing!!")
          
          parameters = quality.getElementsByTagName("Parameter")
          for param in parameters:
            qualityParameters = QualityParameters()
            name = param.getAttributeNode("Name")
            if name != None:
              qualityParameters.setName(name.value.encode('ascii'))
            else:
              gLogger.warn("<Name> Quality XML tag is missing!!")
            
            value = param.getAttributeNode("Value")
            if value != None:
              qualityParameters.setValue(value.value.encode('ascii'))
            else:
              gLogger.warn("<Value> Quality XML tag is missing!!")
            
            fileQuality.addParam(qualityParameters)
          
          outputFile.addQuality(fileQuality)
          
        job.addJobOutputFiles(outputFile)
  
  ########################################################################
  def __readJobSimulationConditions(self, doc, job):
    gLogger.info("Read Simulation Conditions")
    simcond = doc.getElementsByTagName('SimulationCondition')
    if len(simcond) != 1:
      gLogger.warn("To many Simulation conditions!!")
    else:
      simParam = SimulationConditions()
      node = simcond[0]
      parameters = node.getElementsByTagName('Parameter')
      for param in parameters:
        name = param.getAttributeNode('Name')
        value = param.getAttributeNode("Value")
        if name == None or value == None:
          gLogger.warn("<Name>  or <Value> simulation XML tag is missing!!")
        else:
          simParam.addParam(name.value.encode('ascii'), value.value.encode('ascii'))    
          job.addSimulationCond(simParam)  
  
  ########################################################################
  def __readJobSimulationConditions(self, doc, job):
    gLogger.info("Read Simulation Conditions")
    simcond = doc.getElementsByTagName('SimulationCondition')
    if len(simcond) != 1:
      gLogger.warn("To many Simulation conditions!!")
    else:
      simParam = SimulationConditions()
      node = simcond[0]
      parameters = node.getElementsByTagName('Parameter')
      for param in parameters:
        name = param.getAttributeNode('Name')
        value = param.getAttributeNode("Value")
        if name == None or value == None:
          gLogger.warn("<Name>  or <Value> simulation XML tag is missing!!")
        else:
          simParam.addParam(name.value.encode('ascii'), value.value.encode('ascii'))    
          job.addSimulationCond(simParam)  
  
  ########################################################################
  def __readJobDataTakingConditions(self, doc, job):
    gLogger.info("Read DataTaking Conditions")
    DAQcond = doc.getElementsByTagName('DataTakingConditions')
    if len(DAQcond) != 1:
      gLogger.warn("To many DataTaking conditions!!")
    else:
      DAQParam = DataTakingConditions()
      node = DAQcond[0]
      parameters = node.getElementsByTagName('Parameter')
      for param in parameters:
        name = param.getAttributeNode('Name')
        value = param.getAttributeNode("Value")
        if name == None or value == None:
          gLogger.warn("<Name>  or <Value> DataTakingConditions XML tag is missing!!")
        else:
          DAQParam.addParam(name.value.encode('ascii'), value.value.encode('ascii'))    
          job.addDataTakingCond(DAQParam)   