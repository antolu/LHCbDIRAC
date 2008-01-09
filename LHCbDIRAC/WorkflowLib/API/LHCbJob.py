########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/API/LHCbJob.py,v 1.2 2008/01/09 15:45:58 paterson Exp $
# File :   LHCbJob.py
# Author : Stuart Paterson
########################################################################

"""LHCb Job Class

   The LHCb Job class inherits generic functionality from the Job base class
   and provides VO-specific functionality to aid in the construction of
   workflows.

   Helper functions are documented with example usage for the DIRAC API.
"""

__RCSID__ = "$Id: LHCbJob.py,v 1.2 2008/01/09 15:45:58 paterson Exp $"

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
    self.softwareDistribution = 'WorkflowLib.Utilities.SoftwareDistribution'

  #############################################################################
  def setApplication(self,appName,appVersion,optionsFile='',logFile=''):
    """Helper function.

       Specify application for DIRAC workflows.

       For LHCb these could be e.g. Gauss, Boole, Brunel,
       DaVinci, Bender, etc.

       The options file for the application step can also be specified, if not
       given, the default application options file will be used.  If this is
       specified, the file is automatically appended to the job input sandbox.

       Example usage:

       >>> job = Job()
       >>> job.setApplication('DaVinci','v19r5')

       @param appName: Application name
       @type appMame: string
       @param appVersion: Application version
       @type appVersion: string
       @param logFile: Optional log file name
       @type appVersion: string
       @param optionsFile: Can specify options file for application here
       @type appVersion: string
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
    inputParamNames = ['appName','appVersion','optionsFile','optionsLine','systemConfig','logfile']
    for p in inputParamNames:
      step.appendParameterCopy(module.findParameter(p))

    moduleInstance = step.createModuleInstance('GaudiApplication',moduleName)
    for p in inputParamNames:
      moduleInstance.findParameter(p).link('self',p)

    outputParamNames = ['result']
    for p in outputParamNames:
      outputParam = moduleInstance.findParameter(p)
      step.appendParameter(Parameter(parameter=outputParam))
      step.findParameter(p).link(moduleName,p)

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    for p in inputParamNames:
      self.workflow.appendParameterCopy(step.findParameter(p),stepPrefix)
    for p in outputParamNames:
      self.workflow.appendParameterCopy(step.findParameter(p),stepPrefix)

    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)

    gaudiParams = ParameterCollection()
    for p in inputParamNames:
      gaudiParams.append(moduleInstance.findParameter(p))

    stepInstance.linkParameterUp(gaudiParams,stepPrefix)

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

    # now we have to tell DIRAC to install the necessary software
    swDistName = 'SoftwareDistModule'
    currentApp = '%s.%s' %(appName,appVersion)
    if not self.workflow.findParameter(swDistName):
      description='Path for ModuleFactory to find LHCb Software Distribution module'
      self._addParameter(self.workflow,swDistName,'JDL',self.softwareDistribution,description)
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
    """Helper function.

       Specify additional software packages to be installed on Grid
       Worker Node before job execution commences.

       Example usage:

       >>> job = Job()
       >>> job.addPackage('DaVinci','v17r6')

       @param pname: Package name
       @type pname: string
       @param pversion: Package version
       @type pversion: Package version string

    """
    print 'To implement addPackage()'

  #############################################################################
  def setAncestorDepth(self,depth):
    """Helper function.

       Level at which ancestor files are retrieved from the bookkeeping.

       For analysis jobs running over RDSTs the ancestor depth may be specified
       to ensure that the parent DIGI / DST files are staged before job execution.
    """
    if type(depth)==type(1):
      description = 'Level at which ancestor files are retrieved from the bookkeeping'
      self._addParameter(self.workflow,'AncestorDepth','JDL',depth,description)
    else:
      raise TypeError,'Expected Integer for Ancestor Depth'

  #############################################################################
  def setOption(self,optsLine):
    """Helper function.

       For LHCb Gaudi Applications, may add options to be appended
       to application options file.  This must be a triple quoted string,
       e.g." " " ApplicationMgr.EvtMax=-1; " " "

       Example usage:

       >>> job = Job()
       >>> job.setOption('ApplicationMgr.EvtMax=-1;')

       @param optsLine: Options line
       @param optsLine: string
    """
    if type(optsLine)==type(' '):
      prefix = self.__getCurrentStepPrefix()
      if not prefix:
        raise TypeError,'Cannot set options line without specified application step defined'
      self.workflow.findParameter('%soptionsLine' %(prefix)).setValue(optsLine)
    else:
      raise TypeError,'Expected string for Job Options line'

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#