########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Client/LHCbSAMJob.py,v 1.1 2008/07/16 21:13:15 paterson Exp $
# File :   LHCbSAMJob.py
# Author : Stuart Paterson
########################################################################

"""LHCb SAM Job Class

   The LHCb SAM Job class inherits generic VO functionality from the Job base class
   and provides VO-specific functionality to aid in the construction of
   workflows.

   Helper functions are documented with example usage for the DIRAC API.

   Below are several examples of LHCbSAMJob usage.

   An example SAM Test script would be::

     from DIRAC.Interfaces.API.Dirac import Dirac
     from DIRAC.LHCbSystem.Testing.SAM.Client.LHCbSAMJob import LHCbSAMJob

     j = LHCbSAMJob()
     j.setCPUTime(5000)
    #j.setDestination('LCG.CERN.ch')
     j.setSharedAreaLock(forceDeletion=False,enableFlag=False)
     j.checkSystemConfiguration(False)
     j.checkSiteQueues(False)

     dirac = Dirac()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID
"""

__RCSID__ = "$Id: LHCbSAMJob.py,v 1.1 2008/07/16 21:13:15 paterson Exp $"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job
from DIRAC.Core.Utilities.File                      import makeGuid
from DIRAC                                          import gConfig

COMPONENT_NAME='DIRAC/LHCbSystem/Testing/SAM/Client/LHCbSAMJob'

class LHCbSAMJob(Job):

  #############################################################################

  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self,script)
    self.gaudiStepCount = 0
    self.currentStepPrefix = ''
    self.samLogLevel = gConfig.getValue('/Operations/SAM/LogLevel','verbose')
    self.samDefaultCPUTime = gConfig.getValue('/Operations/SAM/CPUTime',50000)
    self.samPlatform = gConfig.getValue('/Operations/SAM/Platform','gLite')
    self.samOutputFiles = gConfig.getValue('/Operations/SAM/OutputSandbox',['*.log'])
    self.__setDefaults()

  #############################################################################
  def __setDefaults(self):
    """ Set some SAM specific defaults.
    """
    self.setLogLevel(self.samLogLevel)
    self.setCPUTime(self.samDefaultCPUTime)
    self.setPlatform(self.samPlatform)
    self.setOutputSandbox(self.samOutputFiles)
    self._addJDLParameter('PilotType','private')

  #############################################################################
  def setSharedAreaLock(self,forceDeletion=False,enableFlag=True):
    """Helper function.

       Add the LockSharedArea test.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.setSharedAreaLock()

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param forceDeletion: Flag to force SAM lock file deletion
       @type forceDeletion: boolean
    """
    if not enableFlag in [True,False] or not forceDeletion in [True,False]:
      raise TypeError,'Expected boolean value for SAM lock test flags'

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %('SAM',stepNumber)
    step =  self.__getSAMLockStep(stepDefn)

    self._addJDLParameter('SAMLockTest',str(enableFlag))
    stepName = 'Run%sStep%s' %('SAM',stepNumber)
    self.addToOutputSandbox.append('*.log')
    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
    stepInstance.setValue("enable",enableFlag)
    stepInstance.setValue("forceLockRemoval",forceDeletion)

  #############################################################################
  def __getSAMLockStep(self,name='LockSharedArea'):
    """Internal function.

        This method controls the definition for a LockSharedArea step.
    """
    # Create the GaudiApplication module first
    moduleName = 'LockSharedArea'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module to manage the lock in the shared area of a Grid site for LHCb')
    body = 'from DIRAC.LHCbSystem.Testing.SAM.Modules.LockSharedArea import LockSharedArea\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('LockSharedArea',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    step.addParameter(Parameter("forceLockRemoval","","bool","","",False, False, "lock deletion flag"))
    return step

  #############################################################################
  def checkSystemConfiguration(self,enableFlag=True):
    """Helper function.

       Add the SystemConfiguration test.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.addSystemConfigurationTest('True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
    """
    if not enableFlag in [True,False]:
      raise TypeError,'Expected boolean value for enableFlag'

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %('SAM',stepNumber)
    step =  self.__getSystemConfigStep(stepDefn)

    self._addJDLParameter('SystemConfigurationTest',str(enableFlag))
    stepName = 'Run%sStep%s' %('SAM',stepNumber)
#    logPrefix = 'Step%s_' %(stepNumber)
#    logName = '%s%s' %(logPrefix,'SystemConfiguration')
    self.addToOutputSandbox.append('*.log')

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
    stepInstance.setValue("enable",enableFlag)

  #############################################################################
  def __getSystemConfigStep(self,name='SystemConfiguration'):
    """Internal function.

        This method controls the definition for a SystemConfiguration step.
    """
    # Create the GaudiApplication module first
    moduleName = 'SystemConfiguration'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module to check the system configuration of a Grid site for LHCb')
    body = 'from DIRAC.LHCbSystem.Testing.SAM.Modules.SystemConfiguration import SystemConfiguration\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('SystemConfiguration',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    return step

  #############################################################################
  def checkSiteQueues(self,enableFlag=True):
    """Helper function.

       Add the SiteQueues test.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.checkSiteQueues('True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean

    """
    if not enableFlag in [True,False]:
      raise TypeError,'Expected boolean value for enableFlag'

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %('SAM',stepNumber)
    step =  self.__getSiteQueuesStep(stepDefn)

    self._addJDLParameter('SiteQueuesTest',str(enableFlag))
    stepName = 'Run%sStep%s' %('SAM',stepNumber)
    self.addToOutputSandbox.append('*.log')
    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
    stepInstance.setValue("enable",enableFlag)

  #############################################################################
  def __getSiteQueuesStep(self,name='SiteQueues'):
    """Internal function.

        This method controls the definition for a SiteQueues step.
    """
    # Create the GaudiApplication module first
    moduleName = 'SiteQueues'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module to check the LHCb queues for the given CE')
    body = 'from DIRAC.LHCbSystem.Testing.SAM.Modules.SiteQueues import SiteQueues\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('SiteQueues',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    return step


  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#