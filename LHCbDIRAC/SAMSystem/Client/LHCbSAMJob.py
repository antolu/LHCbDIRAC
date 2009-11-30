########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/LHCbSystem/Testing/SAM/Client/LHCbSAMJob.py $
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
    from LHCbDIRAC.LHCbSystem.Testing.SAM.Client.LHCbSAMJob import LHCbSAMJob

    j = LHCbSAMJob()
    j.setDestinationCE('LCG.PIC.es')
    j.setSharedAreaLock(forceDeletion=False,enableFlag=False)
    j.checkSystemConfiguration(enableFlag=False)
    j.checkSiteQueues(enableFlag=False)
    j.installSoftware(forceDeletion=False,enableFlag=False)
    j.reportSoftware(enableFalg=False)
    j.testApplications(enableFlag=False)
    j.finalizeAndPublish(logUpload=False,publishResults=False,enableFlag=False)
    dirac = Dirac()
    print j._toJDL()
    jobID = dirac.submit(j,mode='Local')
    print 'Submission Result: ',jobID
"""

__RCSID__ = "$Id: LHCbSAMJob.py 18191 2009-11-11 16:46:41Z paterson $"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job
from DIRAC.Core.Utilities.SiteCEMapping             import getSiteForCE
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
    self.samPlatform = gConfig.getValue('/Operations/SAM/Platform','gLite-SAM')
    self.samGroup = gConfig.getValue('/Operations/SAM/JobGroup','SAM')
    self.samType = gConfig.getValue('/Operations/SAM/JobType','SAM')
    #self.samOwnerGroup = gConfig.getValue('/Operations/SAM/OwnerGroup','lhcb_admin')
    self.samOutputFiles = gConfig.getValue('/Operations/SAM/OutputSandbox',['*.log'])
    self.appTestPath = '/Operations/SAM/TestApplications'
    self.samPriority = gConfig.getValue('/Operations/SAM/Priority',1)
    self.importLine = """
try:
  from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.<MODULE> import <MODULE>
except Exception,x:
  print 'Could not import <MODULE> from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules:\n%s' %(x)
"""
    self.__setDefaults()

  #############################################################################
  def __setDefaults(self):
    """ Set some SAM specific defaults.
    """
    self.setLogLevel(self.samLogLevel)
    self.setCPUTime(self.samDefaultCPUTime)
    self.setPlatform(self.samPlatform)
    self.setOutputSandbox(self.samOutputFiles)
    self.setJobGroup(self.samGroup)
    #self.setOwnerGroup(self.samOwnerGroup)
    self.setType(self.samType)
    self.setPriority(self.samPriority)
    self._addJDLParameter('SubmitPools','SAM')

  #############################################################################
  def setSAMGroup(self,samGroup):
    """ Helper function. Set the SAM group and pilot types as required.

        Example usage:

        >>> job = LHCbSAMJob()
        >>> job.setSAMGroup('SAMsw')

        @param: SAM job group
        @param: string
    """
    if not samGroup in ('SAM','SAMsw'):
      raise TypeError,'Expected SAM / SAMsw for SAM group'

    if samGroup=='SAM':
      self.setJobGroup('SAM')
    else:
      self.setJobGroup('SAMsw')
      self._addJDLParameter('PilotTypes','private')

  #############################################################################
  def setPriority(self,priority):
    """Helper function.

       Add the priority.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.setPriority(10)

       @param priority: User priority
       @type priority: Int
    """
    if not type(priority)==int:
      try:
        priority=int(priority)
      except Exception,x:
        raise TypeError,'Expected Integer for User priority'

    self._addParameter(self.workflow,'Priority','JDL',priority,'User Job Priority')


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

    if forceDeletion:
      self._addJDLParameter('LockRemovalFlag','True')

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
    body = string.replace(self.importLine,'<MODULE>','LockSharedArea')
    #'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.LockSharedArea import LockSharedArea\n'
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

    if enableFlag:
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
    body = string.replace(self.importLine,'<MODULE>','SystemConfiguration')
    #'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.SystemConfiguration import SystemConfiguration\n'
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

    if enableFlag:
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
    body = string.replace(self.importLine,'<MODULE>','SiteQueues')
    #'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.SiteQueues import SiteQueues\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('SiteQueues',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    return step

  #############################################################################
  def installSoftware(self,forceDeletion=False,enableFlag=True,installProjectURL=None):
    """Helper function.

       Add the SoftwareInstallation test.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.installSoftware(enableFlag='True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param forceDeletion: Flag to force shared area deletion e.g. rm -rf *
       @type forceDeletion: boolean
    """
    if not enableFlag in [True,False] or not forceDeletion in [True,False]:
      raise TypeError,'Expected boolean value for SAM lock test flags'

    if installProjectURL:
      if not type(installProjectURL)==type(" "):
        raise TypeError,'Expected string for install_project URL'

    self._addJDLParameter('SoftwareInstallationTest',str(enableFlag))
    if enableFlag:
      self.gaudiStepCount +=1
      stepNumber = self.gaudiStepCount
      stepDefn = '%sStep%s' %('SAM',stepNumber)
      step =  self.__getSoftwareInstallationStep(stepDefn)

      stepName = 'Run%sStep%s' %('SAM',stepNumber)
      self.addToOutputSandbox.append('*.log')
      self.workflow.addStep(step)
      stepPrefix = '%s_' % stepName
      self.currentStepPrefix = stepPrefix
      self._addJDLParameter('DeleteSharedArea',str(forceDeletion))

    # Define Step and its variables
      stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
      stepInstance.setValue("enable",enableFlag)
      stepInstance.setValue("purgeSharedAreaFlag",forceDeletion)

      if installProjectURL:
        self._addJDLParameter('installProjectURL',str(installProjectURL))
        stepInstance.setValue("installProjectURL",installProjectURL)

  #############################################################################
  def __getSoftwareInstallationStep(self,name='SoftwareInstallation'):
    """Internal function.

        This method controls the definition for a SoftwareInstallation step.
    """
    # Create the SoftwareInstallation module first
    moduleName = 'SoftwareInstallation'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module to install LHCb software')
    body = string.replace(self.importLine,'<MODULE>','SoftwareInstallation')
    #'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.SoftwareInstallation import SoftwareInstallation\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('SoftwareInstallation',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    step.addParameter(Parameter("purgeSharedAreaFlag","","bool","","",False, False, "Remove all software in shared area"))
    step.addParameter(Parameter("installProjectURL","","string","","",False, False, "Optional install_project URL"))
    return step

  #############################################################################
  def reportSoftware(self,enableFlag=True,installProjectURL=None):
    """Helper function.

       Add the reportSoftware step.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.reportSoftware('True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean

    """
    if not enableFlag in [True,False]:
      raise TypeError,'Expected boolean value for enableFlag'

    self._addJDLParameter('ReportSoftwareTest',str(enableFlag))
    if enableFlag:
      self.gaudiStepCount +=1
      stepNumber = self.gaudiStepCount
      stepDefn = '%sStep%s' %('SAM',stepNumber)
      step =  self.__getReportSoftwareStep(stepDefn)

      stepName = 'Run%sStep%s' %('SAM',stepNumber)
      self.addToOutputSandbox.append('*.log')
      self.workflow.addStep(step)
      stepPrefix = '%s_' % stepName
      self.currentStepPrefix = stepPrefix

    # Define Step and its variables
      stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
      stepInstance.setValue("enable",enableFlag)

      if installProjectURL:
        self._addJDLParameter('installProjectURL',str(installProjectURL))
        stepInstance.setValue("installProjectURL",installProjectURL)

  #############################################################################
  def __getReportSoftwareStep(self,name='ReportSoftware'):
    """Internal function.

        This method controls the definition for a TestApplications step.
    """
    # Create the GaudiApplication module first
    moduleName = 'SoftwareReport'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module to check the content of the SHARED area for the given CE')
    body = string.replace(self.importLine,'<MODULE>','SoftwareReport')
    #body = 'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.TestApplications import TestApplications\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('SoftwareReport',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    step.addParameter(Parameter("samTestName","","string","","",False, False, "TestApplication SAM Test Name"))
    return step

  #############################################################################
  def testApplications(self,enableFlag=True):
    """Helper function.

       Add the TestApplications step.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.testApplications('True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean

    """
    if not enableFlag in [True,False]:
      raise TypeError,'Expected boolean value for enableFlag'

    if enableFlag:
      result = gConfig.getOptionsDict(self.appTestPath)
      if not result['OK']:
        raise TypeError,'Section %s is not defined or could not be retrieved' %self.appTestPath
      testList = gConfig.getValue('/Operations/SAM/ApplicationTestList',[])
      if not testList:
        raise TypeError,'Could not get list of tests from /Operations/SAM/ApplicationTestList'

      self.log.verbose('Will generate tests for: %s' %(string.join(testList,', ')))
      for testName in testList:
        appNameVersion = result['Value'][testName]
        appNameOptions = result['Value'][testName+'-opts']
        self.gaudiStepCount +=1
        stepNumber = self.gaudiStepCount
        stepDefn = '%sStep%s' %('SAM',stepNumber)
        step =  self.__getTestApplicationsStep(stepDefn)

        self._addJDLParameter('TestApplication%s' %(appNameVersion.replace('.','')),str(enableFlag))
        stepName = 'Run%sStep%s' %('SAM',stepNumber)
        self.addToOutputSandbox.append('*.log')
        self.workflow.addStep(step)
        stepPrefix = '%s_' % stepName
        self.currentStepPrefix = stepPrefix

      # Define Step and its variables
        stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
        stepInstance.setValue("enable",enableFlag)
        stepInstance.setValue("samTestName",testName)
        stepInstance.setValue("appNameVersion",appNameVersion)
        stepInstance.setValue("appNameOptions",appNameOptions)

  #############################################################################
  def __getTestApplicationsStep(self,name='TestApplications'):
    """Internal function.

        This method controls the definition for a TestApplications step.
    """
    # Create the GaudiApplication module first
    moduleName = 'TestApplications'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module to check the LHCb queues for the given CE')
    body = string.replace(self.importLine,'<MODULE>','TestApplications')
    #body = 'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.TestApplications import TestApplications\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('TestApplications',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    step.addParameter(Parameter("samTestName","","string","","",False, False, "TestApplication SAM Test Name"))
    step.addParameter(Parameter("appNameVersion","","string","","",False, False, "Appliciation name and version"))
    step.addParameter(Parameter("appNameOptions","","string","","",False, False, "Appliciation Options"))
    return step

  #############################################################################
  def checkApplications(self,enableFlag=True):
    """Helper function.

       Add the checkApplications step.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.checkApplications('True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean

    """
    if not enableFlag in [True,False]:
      raise TypeError,'Expected boolean value for enableFlag'

    if enableFlag:
      self.gaudiStepCount +=1
      stepNumber = self.gaudiStepCount
      stepDefn = '%sStep%s' %('SAM',stepNumber)
      step =  self.__getCheckApplicationsStep(stepDefn)

      self._addJDLParameter('TestApplication%s' %(appNameVersion.replace('.','')),str(enableFlag))
      stepName = 'Run%sStep%s' %('SAM',stepNumber)
      self.addToOutputSandbox.append('*.log')
      self.workflow.addStep(step)
      stepPrefix = '%s_' % stepName
      self.currentStepPrefix = stepPrefix

      # Define Step and its variables
      stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
      stepInstance.setValue("enable",enableFlag)
      stepInstance.setValue("samTestName",testName)
      stepInstance.setValue("appNameVersion",appNameVersion)

  #############################################################################
  def __getCheckApplicationsStep(self,name='TestApplications'):
    """Internal function.

        This method controls the definition for a TestApplications step.
    """
    # Create the GaudiApplication module first
    moduleName = 'CheckApplications'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module to check if the application is properly installed')
    body = string.replace(self.importLine,'<MODULE>','CheckApplications')
    #body = 'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.TestApplications import TestApplications\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('CheckApplications',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    step.addParameter(Parameter("samTestName","","string","","",False, False, "CheckApplication SAM Test Name"))
    step.addParameter(Parameter("appNameVersion","","string","","",False, False, "Appliciation name and version"))
    return step

  #############################################################################
  def finalizeAndPublish(self,logUpload=True,publishResults=True,enableFlag=True):
    """Helper function.

       Add the SAM Finalization module step.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.finalizeAndPublish(enableFlag='True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param logUpload: Flag to enable / disable log files upload
       @type logUpload: boolean
       @param publishResults: Flag to enable / disable publishing of results to SAM DB
       @type publishResults: boolean
    """
    if not enableFlag in [True,False] or not logUpload in [True,False] or not publishResults in [True,False]:
      raise TypeError,'Expected boolean value for SAM lock test flags'

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %('SAM',stepNumber)
    step =  self.__getSAMFinalizationStep(stepDefn)

    self._addJDLParameter('FinalizeAndPublishTest',str(enableFlag))
    stepName = 'Run%sStep%s' %('SAM',stepNumber)
    self.addToOutputSandbox.append('*.log')
    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
    stepInstance.setValue("enable",enableFlag)
    stepInstance.setValue("publishResultsFlag",publishResults)
    stepInstance.setValue("uploadLogsFlag",logUpload)

  #############################################################################
  def __getSAMFinalizationStep(self,name='SAMFinalization'):
    """Internal function.

        This method controls the definition for a SAMFinalization step.
    """
    # Create the SAMFinalization module first
    moduleName = 'SAMFinalization'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module for LHCb SAM job finalization, reports to SAM DB')
    body = string.replace(self.importLine,'<MODULE>','SAMFinalization')
    #body = 'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.SAMFinalization import SAMFinalization\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('SAMFinalization',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    step.addParameter(Parameter("publishResultsFlag","","bool","","",False, False, "Flag to trigger publishing of results to SAM DB"))
    step.addParameter(Parameter("uploadLogsFlag","","bool","","",False, False, "Flag to trigger upload of SAM logs to LogSE"))
    return step

  #############################################################################
  def runTestScript(self,scriptName='',enableFlag=True):
    """Helper function.

       Add the optional SAM Run Test Script module step to run an arbitrary
       python script as part of the SAM test job.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.runTestScript('myPythonScript.py',enableFlag='True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param scriptName: Path to python script to execute
       @type scriptName: string
    """
    if not enableFlag in [True,False]:
      raise TypeError,'Expected boolean value for enableFlag'
    if not type(scriptName)==type(" ") or not scriptName:
      raise TypeError,'Expected string type for script name'
    if not os.path.exists(scriptName):
      raise TypeError,'Path to script %s must exist' %(scriptName)

    self.addToInputSandbox.append(scriptName)

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %('SAM',stepNumber)
    step =  self.__getRunTestScriptStep(stepDefn)

    self._addJDLParameter('RunTestScriptTest',str(enableFlag))
    stepName = 'Run%sStep%s' %('SAM',stepNumber)
    self.addToOutputSandbox.append('*.log')

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
    stepInstance.setValue("enable",enableFlag)
    stepInstance.setValue("scriptName",os.path.basename(scriptName))

  #############################################################################
  def __getRunTestScriptStep(self,name='RunTestScript'):
    """Internal function.

        This method controls the definition for a RunTestScript step.
    """
    # Create the RunTestScript module first
    moduleName = 'RunTestScript'
    module = ModuleDefinition(moduleName)
    module.setDescription('A module for LHCb SAM job finalization, reports to SAM DB')
    body = string.replace(self.importLine,'<MODULE>','RunTestScript')
    #body = 'from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.RunTestScript import RunTestScript\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('RunTestScript',name)
    # Define step parameters
    step.addParameter(Parameter("enable","","bool","","",False, False, "enable flag"))
    step.addParameter(Parameter("scriptName","","string","","",False, False, "script name to execute"))
    return step

  #############################################################################
  def setDestinationCE(self,ceName):
    """ Sets the Grid requirements for the pilot job to be directed to a particular SE.
        At the same time this adds a requirement for the DIRAC site name for matchmaking.
    """
    diracSite = getSiteForCE(ceName)
    if not diracSite['OK']:
      raise TypeError,diracSite['Message']
    if not diracSite['Value']:
      raise TypeError,'No DIRAC site name found for CE %s' %(ceName)

    diracSite = diracSite['Value']
    self.setDestination(diracSite)
    self._addJDLParameter('GridRequiredCEs',ceName)
    self.setName('SAM-%s' %(ceName))
    self.log.verbose('Set GridRequiredCEs to %s and destination to %s' %(ceName,diracSite))

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#