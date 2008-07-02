########################################################################
# $Id: Job.py,v 1.5 2008/07/02 14:21:59 zmathe Exp $
########################################################################

"""

"""

__RCSID__ = "$Id: Job.py,v 1.5 2008/07/02 14:21:59 zmathe Exp $"


class Job:
    

  #############################################################################  
  def __init__(self):
    self.jobConfiguration_ = ""
    self.jobOptions_ = []
    self.jobParameters_ = []
    self.jobInputFiles_ = []
    self.jobOutputfiles_ = []
    self.simulationCond_ = None
    self.jobId_ = -1
    self.name_ = ""
    self.file_name_ = ""
    
  #############################################################################  
  def setJobConfiguration(self, configuration):
    self.jobConfiguration_ = configuration
  
  #############################################################################  
  def getJobConfiguration(self):
    return self.jobConfiguration_

  #############################################################################  
  def addJobOptions(self, jobOption):
    self.jobOptions_ += [jobOption]
  
  #############################################################################  
  def getJobOptions(self):
    return self.jobOptions_
  
  #############################################################################  
  def addJobParams(self, jobParams):
    self.jobParameters_ += [jobParams]

  #############################################################################  
  def getJobParams(self):
    return  self.jobParameters_

  #############################################################################  
  def addJobInputFiles(self, files):
    self.jobInputFiles_ += [files]

  #############################################################################  
  def getJobInputFiles(self):
    return self.jobInputFiles_

  #############################################################################  
  def addJobOutputFiles(self, files):
    self.jobOutputfiles_ += [files]
      
  #############################################################################  
  def getJobOutputFiles(self):
    return self.jobOutputfiles_
  
  #############################################################################  
  def addSimulationCond(self, cond):
    self.simulationCond_ = cond
  
  #############################################################################  
  def getSimulationCond(self):
    return self.simulationCond_

  #############################################################################  
  def setFileName(self, name):
    self.file_name_ = name

  #############################################################################  
  def getFileName(self):
    return self.file_name_

  #############################################################################  
  def setJobId(self, id):
    self.jobId_ = id

  #############################################################################  
  def getJobId(self):
    return self.jobId_
  
  #############################################################################  
  def setJobName(self, name):
    self.name_ = name

  #############################################################################  
  def getJobName(self):
    return self.name_

  #############################################################################  
  def __repr__(self):
    result = "JOB: \n"
    result += str(self.jobConfiguration_)+" " 
    for option in self.jobOptions_:
        result +=  str(option)
    result += '\n'
    for param in  self.jobParameters_:
      result += str(param)
    result += '\n' 
    for input in self.jobInputFiles_:
      result += str(input)   
      
    for output in self.jobOutputfiles_: 
      result +=  str(output)
    result += '\n'
    return result
  
  def writeToXML(self):
    s = ''
    s += '<?xml version="1.0" encoding="ISO-8859-1"?>\n'
    s += '<!DOCTYPE Job SYSTEM "book.dtd">\n'
    
    s +=  self.getJobConfiguration().writeToXML()
    for param in  self.jobParameters_:
      s +=  param.writeToXML()
    
    for input in self.jobInputFiles_:
      s += input.writeToXML()   
    
    for output in self.jobOutputfiles_: 
      s +=  output.writeToXML()
    
    sim = self.getSimulationCond() 
    if sim != None:
      s += sim.writeToXML()
    
    s += '</Job>'
    
    return s
          
      
	#############################################################################  