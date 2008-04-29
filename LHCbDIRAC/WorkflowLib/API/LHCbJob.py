########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/API/LHCbJob.py,v 1.6 2008/04/29 17:19:14 paterson Exp $
# File :   LHCbJob.py
# Author : Stuart Paterson
########################################################################

"""LHCb Job Class

   The LHCb Job class inherits generic VO functionality from the Job base class
   and provides VO-specific functionality to aid in the construction of
   workflows.

   Helper functions are documented with example usage for the DIRAC API.

   This class is under intensive development and currently only provides
   the functionality to construct LHCb project jobs.  The following use-cases
   are pending:
    - Root macros
    - GaudiPython / Bender scripts.

   An example script (for a simple DaVinci job) would be::

     from DIRAC.Interfaces.API.Dirac import Dirac
     from DIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(5000)
     j.setSystemConfig('slc4_ia32_gcc34')
     j.setApplication('DaVinci','v19r11','DaVinciv19r11.opts')
     j.setInputData(['/lhcb/production/DC06/phys-lumi2/00001501/DST/0000/00001501_00000320_5.dst'])
     j.setName('MyJobName')
     #j.setDestination('LCG.CERN.ch')

     dirac = Dirac()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID

   The setDestination() method is optional and takes the DIRAC site name as an argument.
"""

__RCSID__ = "$Id: LHCbJob.py,v 1.6 2008/04/29 17:19:14 paterson Exp $"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job

COMPONENT_NAME='/WorkflowLib/API/LHCbJob'

class LHCbJob(Job):

  #############################################################################

  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self,script)
    self.gaudiStepCount = 0
    self.currentStepPrefix = ''
    self.inputDataType = 'DATA' #Default, other options are MDF, ETC

  #############################################################################
  def setApplication(self,appName,appVersion,optionsFile='',logFile=''):
    """Helper function.

       Specify application for DIRAC workflows.

       For LHCb these could be e.g. Gauss, Boole, Brunel,
       DaVinci, Bender, etc.

       The options file for the application step can also be specified, if not
       given, the default application options file will be used.  If this is
       specified, the file is automatically appended to the job input sandbox.

       Any additional options files should be specified in the job...

       Example usage:

       >>> job = LHCbJob()
       >>> job.setApplication('DaVinci','v19r5',optionsFile='MyDV.opts',logFile='dv.log')

       @param appName: Application name
       @type appName: string
       @param appVersion: Application version
       @type appVersion: string
       @param logFile: Optional log file name
       @type logFile: string
       @param optionsFile: Can specify options file for application here
       @type optionsFile: string
    """
    if not type(appName) == type(' ') or not type(appVersion) == type(' '):
      raise TypeError,'Expected strings for application name and version'

    if logFile:
      if type(logFile) == type(' '):
        logName = logFile
      else:
        raise TypeError,'Expected string for log file name'
    else:
      logName = '%s_%s.log' %(appName,appVersion)

    self.gaudiStepCount +=1
    module =  self.__getGaudiApplicationModule()

    moduleName = '%s%s' %(appName,appVersion)
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %(appName,stepNumber)
    stepName = 'Run%sStep%s' %(appName,stepNumber)

    logPrefix = 'Step%s_' %(stepNumber)
    logName = '%s%s' %(logPrefix,logName)
    self.addToOutputSandbox.append(logName)

    step = StepDefinition(stepDefn)
    step.addModule(module)
    #TODO: add links to input data and set value for input data type
    inputParamNames = ['appName','appVersion','optionsFile','optionsLine','systemConfig','logfile']
    for p in inputParamNames:
      step.addParameter(module.findParameter(p))

    step.addParameter(module.findParameter('inputData'))
    step.addParameter(module.findParameter('inputDataType'))

    moduleInstance = step.createModuleInstance('GaudiApplication',moduleName)
    for p in inputParamNames:
      moduleInstance.findParameter(p).link('self',p)

    moduleInstance.findParameter('inputData').link('self','inputData')
    moduleInstance.findParameter('inputDataType').link('self','inputDataType')

    outputParamNames = ['result']
    for p in outputParamNames:
      outputParam = moduleInstance.findParameter(p)
      step.addParameter(Parameter(parameter=outputParam))
      step.findParameter(p).link(moduleName,p)

    step.findParameter('inputData').link(moduleName,'inputData')
    step.findParameter('inputDataType').link(moduleName,'inputDataType')

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    for p in inputParamNames:
      self.workflow.addParameter(step.findParameter(p),stepPrefix)
    for p in outputParamNames:
      self.workflow.addParameter(step.findParameter(p),stepPrefix)

    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)

    gaudiParams = ParameterCollection()
    for p in inputParamNames:
      gaudiParams.append(moduleInstance.findParameter(p))

    stepInstance.linkUp(gaudiParams,stepPrefix)
    stepInstance.setLink("inputData","self", "InputData")
    stepInstance.setLink("inputDataType","self", "InputDataType")
    self.workflow.findParameter('%sappName' %(stepPrefix)).setValue(appName)
    self.workflow.findParameter('%sappVersion' %(stepPrefix)).setValue(appVersion)
    self.workflow.findParameter('%slogfile' %(stepPrefix)).setValue(logName)

    if not optionsFile:
      self.workflow.findParameter('%soptionsFile' %(stepPrefix)).setValue('')
    elif os.path.exists(optionsFile):
      self.workflow.findParameter('%soptionsFile' %(stepPrefix)).setValue(os.path.basename(optionsFile))
      self.addToInputSandbox.append(optionsFile)
    else:
      raise TypeError,'Specified options file should exist locally'

    if not self.systemConfig:
      raise TypeError, 'Job system configuration (CMTCONFIG) must be specified before application'
    else:
      self.workflow.findParameter('%ssystemConfig' %(stepPrefix)).setValue(self.systemConfig)

    for p in outputParamNames:
      self.workflow.findParameter('%s%s' %(stepPrefix,p)).link(stepInstance.getName(),'%s' %(p))

    # set the value for input data type (can be overidden explicitly)
    description = 'Default input data type field'
    self._addParameter(self.workflow,'InputDataType','JDL',self.inputDataType,description)

    # now we have to tell DIRAC to install the necessary software
    currentApp = '%s.%s' %(appName,appVersion)
    swPackages = 'SoftwarePackages'
    description='List of LHCb Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)

  #############################################################################
  def __getGaudiApplicationModule(self):
    """Internal function.

      This method controls the definition for a GaudiApplication module.
    """
    moduleName = 'GaudiApplication'
    module = ModuleDefinition(moduleName)
    self._addParameter(module,'appName','Parameter','string','Application Name')
    self._addParameter(module,'appVersion','Parameter','string','Application Version')
    self._addParameter(module,'optionsFile','Parameter','string','Options File Name')
    self._addParameter(module,'optionsLine','String','string','Options Line')
    self._addParameter(module,'systemConfig','Parameter','string','CMTCONFIG Value')
    self._addParameter(module,'logfile','Parameter','string','Log File Name')
    self._addParameter(module,'result','Parameter','string','Execution Result',io='output')
    self._addParameter(module,'inputData','Parameter','','Input data')
    self._addParameter(module,'inputDataType','String','','Automatically resolved input data type')
    module.setDescription('A generic Gaudi Application module that can execute any provided project name and version')
    body = 'from WorkflowLib.Module.GaudiApplication import GaudiApplication\n'
    module.setBody(body)
    return module

  #############################################################################
  def __getCurrentStepPrefix(self):
    """Internal function, returns current step prefix for setting parameters.
    """
    return self.currentStepPrefix

  #############################################################################
  def addPackage(self,pname,pversion):
    """Under development. Helper function.

       Specify additional software packages to be installed on Grid
       Worker Node before job execution commences.

       Example usage:

       >>> job = LHCbJob()
       >>> job.addPackage('DaVinci','v17r6')

       @param pname: Package name
       @type pname: string
       @param pversion: Package version
       @type pversion: Package version string

    """
    print 'To implement addPackage(), pending update to install_project to cope with local + shared installations'

  #############################################################################
  def setAncestorDepth(self,depth):
    """Helper function.

       Level at which ancestor files are retrieved from the bookkeeping.

       For analysis jobs running over RDSTs the ancestor depth may be specified
       to ensure that the parent DIGI / DST files are staged before job execution.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setAncestorDepth(2)

       @param depth: Ancestor depth
       @type depth: string or int

    """
    description = 'Level at which ancestor files are retrieved from the bookkeeping'
    if type(depth)==type(" "):
      try:
        self._addParameter(self.workflow,'AncestorDepth','JDL',int(depth),description)
      except Exception,x:
        raise TypeError,'Expected integer for Ancestor Depth'
    elif type(depth)==type(1):
      self._addParameter(self.workflow,'AncestorDepth','JDL',depth,description)
    else:
      raise TypeError,'Expected Integer for Ancestor Depth'

  #############################################################################
  def setInputDataType(self,inputDataType):
    """Helper function.

       Explicitly set the input data type to be conveyed to Gaudi Applications.

       Default is DATA, e.g. for DST / RDST files.  Other options include:
        - MDF, for .raw files
        - ETC, for running on a public or private Event Tag Collections.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setInputDataType('ETC')

       @param inputDataType: Input Data Type
       @type inputDataType: String

    """
    description = 'User specified input data type'
    if not type(inputDataType)==type(" "):
      try:
        inputDataType = str(inputDataType)
      except Exception,x:
        raise TypeError,'Expected string for input data type'

    self.inputDataType = inputDataType
    self._addParameter(self.workflow,'InputDataType','JDL',inputDataType,description)

  #############################################################################
  def setCondDBTags(self,condDict):
    """Under development. Helper function.

       Specify Conditions Database tags by by Logical File Name (LFN).

       The input dictionary is of the form: {<DB>:<TAG>} as in the example below.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setCondDBTags({'DDDB':'DC06','LHCBCOND':'DC06'})

       @param condDict: CondDB tags
       @type condDict: Dict of DB, tag pairs
    """
    if not type(condDict)==type({}):
      raise TypeError, 'Expected dictionary for CondDB tags'

    conditions = []
    for db,tag in condDict.items():
      try:
        db = str(db)
        tag = str(tag)
        conditions.append(string.join([db,tag],'.'))
      except Exception,x:
        raise TypeError,'Expected string for conditions'

    condStr = string.join(conditions,';')
    description = 'List of CondDB tags'
    self._addParameter(self.workflow,'CondDBTags','JDL',condStr,description)

  #############################################################################
  def setOption(self,optsLine):
    """Under development. Helper function.

       For LHCb Gaudi Applications, may add options to be appended
       to application options file.  This must be a triple quoted string if multiple lines,
       are to be added e.g." " " ApplicationMgr.EvtMax=-1; " " "

       Example usage:

       >>> job = LHCbJob()
       >>> job.setOption('ApplicationMgr.EvtMax=-1;')

       @param optsLine: Options line
       @type optsLine: string
    """
    if type(optsLine)==type(' '):
      prefix = self.__getCurrentStepPrefix()
      if not prefix:
        raise TypeError,'Cannot set options line without specified application step defined'
      self.workflow.findParameter('%soptionsLine' %(prefix)).setValue(optsLine)
    else:
      raise TypeError,'Expected string for Job Options line'

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#