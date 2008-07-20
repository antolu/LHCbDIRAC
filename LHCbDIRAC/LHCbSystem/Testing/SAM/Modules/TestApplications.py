########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/TestApplications.py,v 1.1 2008/07/20 16:59:04 paterson Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb TestApplications SAM Module

    Corresponds to SAM tests CE-lhcb-job-*. The tests are defined in CS
    path /Operations/SAM/TestApplications/<TEST NAME> = <APP NAME>.<APP VERSION>

"""

__RCSID__ = "$Id: TestApplications.py,v 1.1 2008/07/20 16:59:04 paterson Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
  from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
  from LHCbSystem.Client.LHCbJob import LHCbJob

from DIRAC.Interfaces.API.Dirac import Dirac

import string, os, sys, re, shutil

SAM_TEST_NAME='' #Defined in the workflow
SAM_LOG_FILE=''  #Defined using workflow parameters

class TestApplications(ModuleBaseSAM):

  #############################################################################
  def __init__(self):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__(self)
    self.version = __RCSID__
    self.logFile = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.site = gConfig.getValue('/LocalSite/Site','LCG.Unknown.ch')
    self.appSystemConfig = gConfig.getValue('/Operations/SAM/AppTestSystemConfig','slc4_ia32_gcc34')
    self.log = gLogger.getSubLogger( "TestApplications" )
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True
    self.samTestName = ''
    self.appNameVersion = ''

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('samTestName'):
      self.testName=self.step_commons['samTestName']

    if self.step_commons.has_key('appNameVersion'):
      self.appNameVersion=self.step_commons['appNameVersion']
      self.logFile='sam-job-%s.log' %(self.appNameVersion.replace('.','-'))

    self.log.verbose('Enable flag is set to %s' %self.enable)
    self.log.verbose('Test Name is: %s' %self.testName)
    self.log.verbose('Application name and version are: %s' %self.appNameVersion)
    self.log.verbose('Log file name is: %s' %self.logFile)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the TestApplications module.
    """
    self.log.info('Initializing '+self.version)
    self.resolveInputVariables()

    self.result = S_OK()
    if not self.testName or not self.appNameVersion or not self.logFile:
      self.result = S_ERROR( 'No application name / version defined' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info('An error was detected in a previous step, exiting.')
      return self.finalize('Problem during execution','Failure detected in a previous step','error')

    self.setApplicationStatus('Starting %s Test' %self.testName)

    options = self.__getOptions(self.appNameVersion.split('.')[0], self.appNameVersion.split('.')[1])
    if not options['OK']:
      return self.finalize('Inputs for %s %s could not be found' %(self.appNameVersion.split('.')[0],self.appNameVersion.split('.')[1]),options['Message'],'critical')

    result = self.__runApplication(self.appNameVersion.split('.')[0],self.appNameVersion.split('.')[1],options['Value'])
    if not result['OK']:
      return self.finalize('Failure during %s %s execution' %(self.appNameVersion.split('.')[0],self.appNameVersion.split('.')[1]),result['Message'],'error')

    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

  #############################################################################
  def __getOptions(self,appName,appVersion):
    """Method to set the correct options for the LHCb project that will be executed.
       By convention the inputs / outputs are the system configuration + file extension.
    """
    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists(sharedArea):
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return self.finalize('Could not determine shared area for site',sharedArea,'critical')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    #Could override these settings using the CS.
    appPaths = {'Gauss':'Sim','Boole':'Digi','Brunel':'Rec','DaVinci':'Phys'}
    appOpts = {'Gauss':'v200601.opts','Boole':'v200601.opts','Brunel':'v200601.opts','DaVinci':'DVOfficialStrippingFile.opts'}

    if not appName in appPaths.keys():
      return S_ERROR('Application options not found')

    optsPath = '%s/lhcb/%s/%s_%s/%s/%s/%s/options/%s' %(sharedArea,appName.upper(),appName.upper(),appVersion,appPaths[appName],appName,appVersion,appOpts[appName])
    self.log.verbose('Looking for %s %s options in path: %s' %(appName,appVersion,optsPath))
    if not os.path.exists(optsPath):
      return S_ERROR('Could not find options file %s' %optsPath)

    localOpts = '%s/%s_%s_%s.opts' %(os.getcwd(),appName,appVersion,self.appSystemConfig)
    shutil.copy(optsPath,localOpts)
    if not os.path.exists(localOpts):
      return S_ERROR('Could not get options file %s' %(localOpts))

    #Nasty but works:
    extraOpts = ''
    if appName=='Gauss':
      extraOpts = """ApplicationMgr.EvtMax = 2;
GaussTape.Output = "DATAFILE='PFN:%s.sim' TYP='POOL_ROOTTREE' OPT='RECREATE'";
""" %(self.appSystemConfig)
    elif appName=='Boole':
      if self.enable:
        if not os.path.exists('%s.sim' %self.appSystemConfig):
          return S_ERROR('No input file %s.sim found for Boole' %(self.appSystemConfig))
      extraOpts = """InitDataSeq.Members -= { "MergeEventAlg/SpilloverAlg" };
EventSelector.Input = {"DATAFILE='PFN:%s.sim' TYP='POOL_ROOTTREE' OPT='READ'"};
DigiWriter.Output = "DATAFILE='PFN:%s.digi' TYP='POOL_ROOTTREE' OPT='REC'";
""" %(self.appSystemConfig,self.appSystemConfig)
    elif appName=='Brunel':
      if self.enable:
        if not os.path.exists('%s.digi' %self.appSystemConfig):
          return S_ERROR('No input file %s.digi found for Brunel' %(self.appSystemConfig))
      extraOpts = """EventSelector.Input = {"DATAFILE='PFN:%s.digi' TYP='POOL_ROOTTREE' OPT='READ'"};
DstWriter.Output = "DATAFILE='PFN:%s.dst' TYP='POOL_ROOTTREE' OPT='REC'";
"""  %(self.appSystemConfig,self.appSystemConfig)
    elif appName=='DaVinci':
      if self.enable:
        if not os.path.exists('%s.dst' %self.appSystemConfig):
          return S_ERROR('No input file %s.dst found for DaVinci' %(self.appSystemConfig))
      extraOpts = """EventSelector.Input = {"DATAFILE='PFN:%s.dst' TYP='POOL_ROOTTREE' OPT='READ'"};
""" %(self.appSystemConfig)

    self.log.verbose('Adding extra options for %s %s:\n%s' %(appName,appVersion,extraOpts))
    fopen = open(localOpts,'a')
    fopen.write('//\n// Options added by TestApplications for DIRAC SAM test %s\n//\n' %(self.testName))
    fopen.write(extraOpts)
    fopen.close()
    return S_OK(localOpts)

  #############################################################################
  def __runApplication(self,appName,appVersion,options):
    """Method to run a test job locally.
    """
    result = S_OK()
    try:
      j = LHCbJob()
      j.setSystemConfig(self.appSystemConfig)
      j.setApplication(appName,appVersion,options,logFile=self.logFile)
      j.setName('%s%sSAMTest' %(appName,appVersion))
      j.setLogLevel(gConfig.getValue('/Operations/SAM/LogLevel','verbose'))
      j.setPlatform(gConfig.getValue('/Operations/SAM/Platform','gLite'))
      self.log.verbose('Job JDL is:\n%s' %(j._toJDL()))
      dirac = Dirac()
      if self.enable:
        result = dirac.submit(j,mode='Local')
    except Exception,x:
      self.log.warn('Problem during %s %s execution: %s' %(appName,appVersion,x))
      return S_ERROR(str(x))
    #Correct the log file names since they will have Step1_ prepended.
    if os.path.exists('Step1_%s' %self.logFile):
      shutil.move('Step1_%s' %self.logFile,self.logFile)
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#