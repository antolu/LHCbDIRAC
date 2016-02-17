########################################################################
# $Id: Job.py 58494 2012-11-14 14:42:46Z zmathe $
########################################################################

"""
stores the jobs and its parameters
"""

__RCSID__ = "$Id$"


class Job:
  """
  Job class
  """

  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.jobConfiguration_ = None
    self.jobOptions_ = []
    self.jobParameters_ = []
    self.jobInputFiles_ = []
    self.jobOutputfiles_ = []
    self.simulationCond_ = None
    self.dataTaking_ = None
    self.jobId_ = -1
    self.name_ = ""
    self.file_name_ = ""

  #############################################################################
  def setJobConfiguration(self, configuration):
    """sets the job configuration"""
    self.jobConfiguration_ = configuration

  #############################################################################
  def getJobConfiguration(self):
    """returns the job configuration"""
    return self.jobConfiguration_

  #############################################################################
  def addJobOptions(self, jobOption):
    """adds the job options"""
    self.jobOptions_ += [jobOption]

  #############################################################################
  def getJobOptions(self):
    """returns the job options"""
    return self.jobOptions_

  #############################################################################
  def addJobParams(self, jobParams):
    """sets the job parameters"""
    self.jobParameters_ += [jobParams]

  #############################################################################
  def removeJobParam(self, paramName):
    """remove a job parameter"""
    self.jobParameters_.remove(paramName)

  #############################################################################
  def getJobParams(self):
    """returns the job parameters"""
    return  self.jobParameters_

  #############################################################################
  def exists(self, jobParam):
    """checks a given job parameter"""
    ok = False
    for i in self.jobParameters_:
      if i.getName() == jobParam:
        ok = True
    return ok

  #############################################################################
  def getParam(self, jobParam):
    """returns a job parameter"""
    param = None
    for i in self.jobParameters_:
      if i.getName() == jobParam:
        param = i
    return param

  #############################################################################
  def removeParam(self, jobParam):
    """removes a job parameter"""
    for i in self.jobParameters_:
      if i.getName() == jobParam:
        self.jobParameters_.remove(i)

  #############################################################################
  def addJobInputFiles(self, files):
    """adds the input file to a job"""
    self.jobInputFiles_ += [files]

  #############################################################################
  def getJobInputFiles(self):
    """returns the input files"""
    return self.jobInputFiles_

  #############################################################################
  def addJobOutputFiles(self, files):
    """adds the output files to a job"""
    self.jobOutputfiles_ += [files]

  #############################################################################
  def getJobOutputFiles(self):
    """returns the output files"""
    return self.jobOutputfiles_

  #############################################################################
  def getOutputFileParam(self, paramName):
    """returns the parameters of a output file"""
    for i in self.jobOutputfiles_:
      param = i.getParam(paramName)
      if param != None:
        return param
    return None

  #############################################################################
  def addSimulationCond(self, cond):
    """sets the simulation condition of a job"""
    self.simulationCond_ = cond

  #############################################################################
  def addDataTakingCond(self, cond):
    """sets the data taking conditions of a job"""
    self.dataTaking_ = cond

  #############################################################################
  def getDataTakingCond(self):
    """returns the data taking conditions"""
    return self.dataTaking_
  #############################################################################

  def getSimulationCond(self):
    """returns the simulation conditions"""
    return self.simulationCond_

  #############################################################################
  def setFileName(self, name):
    """sets the file name"""
    self.file_name_ = name

  #############################################################################
  def getFileName(self):
    """returns the file name"""
    return self.file_name_

  #############################################################################
  def setJobId(self, jobid):
    """sets the job identifier"""
    self.jobId_ = jobid

  #############################################################################
  def getJobId(self):
    """returns the job identifier"""
    return self.jobId_

  #############################################################################
  def setJobName(self, name):
    """sets the job name"""
    self.name_ = name

  #############################################################################
  def getJobName(self):
    """returns the job name"""
    return self.name_

  #############################################################################
  def __repr__(self):
    """formats the output of the print command"""
    result = "JOB: \n"
    result += str(self.jobConfiguration_) + " "
    for option in self.jobOptions_:
      result += str(option)
    result += '\n'
    for param in  self.jobParameters_:
      result += str(param)
    result += '\n'
    for jobinput in self.jobInputFiles_:
      result += str(jobinput)

    for output in self.jobOutputfiles_:
      result += str(output)
    result += '\n'
    return result

  #############################################################################
  def writeToXML(self):
    """writes an XML string"""
    string = ''
    string += '<?xml version="1.0" encoding="ISO-8859-1"?>\n'
    string += '<!DOCTYPE Job SYSTEM "book.dtd">\n'

    string = "%s%s" % (string, self.getJobConfiguration().writeToXML())
    for param in  self.jobParameters_:
      string = "%s%s" % (string, param.writeToXML())

    for inputFile in self.jobInputFiles_:
      string = "%s%s" % (string, inputFile.writeToXML())

    for output in self.jobOutputfiles_:
      string = "%s%s" % (string, output.writeToXML())

    sim = self.getSimulationCond()
    if sim != None:
      string = "%s%s" % (string, sim.writeToXML())

    daq = self.getDataTakingCond()
    if daq != None:
      string = "%s%s" % (string, daq.writeToXML())

    string += '</Job>'

    return string
#############################################################################
