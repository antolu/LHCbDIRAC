########################################################################
# $Id: JobReader.py,v 1.2 2008/02/29 17:51:46 zmathe Exp $
########################################################################

"""

"""

from xml.dom.ext.reader                                                     import Sax
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.JobConfiguration           import JobConfiguration
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.JobOption                  import JobOption
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.File                       import File
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.JobParameters              import JobParameters
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.Job                        import Job
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.FileParam                  import FileParam
from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica                import Replica
from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.ReplicaParam           import ReplicaParam
from DIRAC                                                                  import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: JobReader.py,v 1.2 2008/02/29 17:51:46 zmathe Exp $"


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
    gLogger.info("Job reading from" + str(fileName))
            
    conf = JobConfiguration()
    jobElements = doc.getElementsByTagName('Job')
    for node in jobElements:
    
        name = node.getAttributeNode('ConfigName')
        if name != None:
            conf.setConfigName(name._get_value().encode('ascii'))
        else:
            gLogger.error("Missing the Job ConfigName in the XML file")
        
        version = node.getAttributeNode('ConfigVersion')
        if version != None:
            conf.setConfigVersion(version._get_value().encode('ascii'))
        else:
            gLogger.error("Missing the Job ConfigVersion in the XML file")
        
        date = node.getAttributeNode('Date')
        if date != None:
            conf.setDate(date._get_value().encode('ascii'))
        else:
            gLogger.error("Missing the Job Date in the XML file")
        
        time = node.getAttributeNode('Time') 
        if time != None:
            conf.setTime(time._get_value().encode('ascii'))
        else:
            gLogger.error("Missing the Job Time in the XML file")                
    
    job.setJobConfiguration(conf)
    
    jobOptions = doc.getElementsByTagName('JobOption')
    for node in jobOptions:
        options = JobOption()
        recipient = node.getAttributeNode('Recipient')
        if recipient != None:
            options.setRecipient(recipient._get_value().encode('ascii'))
        else:
            gLogger.warn("Missing the <Recipinet> attribute in job xml!")
        
        name = node.getAttributeNode('Name')._get_value().encode('ascii')
        if name != None:
            options.setName(name)
        
        value = node.getAttributeNode('Value')._get_value().encode('ascii')
        options.setValue(value)
        
        job.addJobOptions(options)
    
    jobTypeParameters = doc.getElementsByTagName('TypedParameter')
    
    for node in jobTypeParameters:
        parameters = JobParameters()
        name =  node.getAttributeNode('Name')._get_value().encode('ascii')
        parameters.setName(name)
        
        value = node.getAttributeNode('Value')._get_value().encode('ascii')
        parameters.setValue(value)
        type = node.getAttributeNode('Type')._get_value().encode('ascii')
        parameters.setType(type)
        job.addJobParams(parameters)      
    
    
    jobInputFile = doc.getElementsByTagName('InputFile')
    for node in jobInputFile:
        inputFile = File()
        input = node.getAttributeNode('Name')._get_value().encode('ascii')
        inputFile.setFileName(input)   
        if inputFile != None:
          job.addJobInputFiles(inputFile)
        else:
          gLogger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      
    
    jobOutputFiles = doc.getElementsByTagName('OutputFile')
    for node in jobOutputFiles:
        outputFile = File()
        
        name = node.getAttributeNode('Name')._get_value().encode('ascii')
        type = node.getAttributeNode('TypeName')._get_value().encode('ascii')
        version = node.getAttributeNode('TypeVersion')._get_value().encode('ascii')
        outputFile.setFileName(name)
        outputFile.setFileType(type)
        outputFile.setFileVersion(version)

        fileparams = node.getElementsByTagName('Parameter')
        for param in fileparams:
            outputFileParams = FileParam()
            
            name = param.getAttributeNode('Name')._get_value().encode('ascii')
            value = param.getAttributeNode('Value')._get_value().encode('ascii')
            outputFileParams.setParamName(name)
            outputFileParams.setParamValue(value)
            outputFile.addFileParam(outputFileParams)
        
        replicas = doc.getElementsByTagName('Replica')
        
        for replica in replicas:
          rep = Replica()
          param = ReplicaParam()
          name = replica.getAttributeNode("Name")
          location = replica.getAttributeNode("Location")
          if name != None:
            param.setName(name._get_value().encode('ascii'))
          if location != None:
            param.setLocation(location._get_value().encode('ascii'))
            rep.addParam(param)
          outputFile.addReplicas(rep)
        
        job.addJobOutputFiles(outputFile)
    
                
    gLogger.info("Job reading fhinished succesfull!")
    return job
    
    
  ########################################################################
