########################################################################
# $Id: ErrorLogging.py 19442 2009-12-10 14:31:27Z paterson $
########################################################################
""" The ErrorLogging module is used to perform error analysis using AppConfig
    utilities. This occurs at the end of each workflow step such that the
    step_commons dictionary can be utilized.
    
    Since not all projects are instrumented to work with the AppConfig 
    error suite any failures will not be propagated to the workflow.
"""

__RCSID__ = "$Id: ErrorLogging.py 19442 2009-12-10 14:31:27Z paterson $"

from LHCbDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC.Core.Utilities.Subprocess                       import shellCall
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

from LHCbDIRAC.Core.Utilities.ProductionEnvironment         import getProjectEnvironment,addCommandDefaults,createDebugScript
from LHCbDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

import os,string,shutil,re,sys

class ErrorLogging(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "ErrorLogging" )
    #Step parameters
    self.enable=True
    self.jobID = ''
    self.applicationName=''
    self.applicationVersion=''
    self.applicationLog=''
    #Workflow commons parameters
    self.request = None
    self.productionID = None
    self.prodJobID = None
    self.systemConfig=''
    #Internal parameters
    self.executable = '$APPCONFIGROOT/scripts/LogErr.py'
    self.errorLogFile = ''
    self.errorLogName = ''
    self.stdError = ''
    #Error log parameters
    self.defaultName = 'errors.html'
    
  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module input parameters are resolved here.
    """
    self.log.debug(self.workflow_commons)
    self.log.debug(self.step_commons)

    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
      self.log.verbose('Found WMS JobID = %s' %self.jobID)
    else:
      self.log.info('No WMS JobID found, module will still attempt to run without publishing to the ErrorLogging service')
      self.jobID = '12345'
      self.enable=False

    if self.step_commons.has_key('Enable'):
      self.enable=self.step_commons['Enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName('job_%s_request.xml' % self.jobID)
      self.request.setJobID(self.jobID)
      self.request.setSourceComponent("Job_%s" % self.jobID)

    if self.workflow_commons.has_key('PRODUCTION_ID'):
      self.productionID=self.workflow_commons['PRODUCTION_ID']

    if self.workflow_commons.has_key('JOB_ID'):
      self.prodJobID=self.workflow_commons['JOB_ID']

    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']

    #Must get all the necessary step parameters
    self.stepNumber = self.step_commons['STEP_NUMBER']
    if self.step_commons.has_key('applicationName'):
       self.applicationName = self.step_commons['applicationName']
    if self.step_commons.has_key('applicationVersion'):
       self.applicationVersion = self.step_commons['applicationVersion']
    if self.step_commons.has_key('applicationLog'):       
       self.applicationLog = self.step_commons['applicationLog']    
    
    if not self.applicationName or not self.applicationVersion or not self.applicationLog:
      return S_ERROR('One of application name, version or log file is null: %s %s %s' %(self.applicationName,self.applicationVersion,self.applicationLog))
    
    if self.step_commons.has_key('extraPackages'):
      self.extraPackages = self.step_commons['extraPackages']   
    
    if not self.extraPackages == '':
      if type(self.extraPackages) != type([]):
        self.extraPackages = self.extraPackages.split(';')
    
    self.errorLogFile = 'Error_Log_%s_%s_%s.log' %(self.applicationName,self.applicationVersion,self.stepNumber)
    self.errorLogName = '%s_Errors_%s_%s_%s.html' %(self.jobID,self.applicationName,self.applicationVersion,self.stepNumber)
    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function. Always return S_OK() because we don't want the
        job execution result to depend on retrieving errors from logs. 
        
        This module will run regardless of the workflow status.
    """
    self.log.info('Initializing %s' %self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.info(result['Message'])
      return S_OK()

    self.log.info('Executing ErrorLogging module for: %s %s %s' %(self.applicationName,self.applicationVersion,self.applicationLog))
    if not os.path.exists(self.applicationLog):
      self.log.info('Application log file from previous module not found locally: %s' %self.applicationLog)
      return S_OK()

    #Now obtain the project environment for execution
    result = getProjectEnvironment(self.systemConfig,self.applicationName,applicationVersion=self.applicationVersion,extraPackages=self.extraPackages)
    if not result['OK']:
      self.log.error('Could not obtain project environment with result: %s' %(result))
      return S_OK()
    
    projectEnvironment = result['Value']
    command = 'python $APPCONFIGROOT/scripts/LogErr.py %s %s %s' %(self.applicationLog,self.applicationName,self.applicationVersion)
    
    #Set some parameter names
    dumpEnvName = 'Environment_Dump_ErrorLogging_Step%s.log' %(self.stepNumber)
    scriptName = 'Error_Log_%s_%s_Run_%s.sh' %(self.applicationName,self.applicationVersion,self.stepNumber)
    coreDumpName = 'ErrorLogging_Step%s' %(self.stepNumber)
    
    #Wrap final execution command with defaults
    finalCommand = addCommandDefaults(command,envDump=dumpEnvName,coreDumpLog=coreDumpName)['Value'] #should always be S_OK()

    #Create debug shell script to reproduce the application execution
    debugResult = createDebugScript(scriptName,command,env=projectEnvironment,envLogFile=dumpEnvName,coreDumpLog=coreDumpName) #will add command defaults internally
    if debugResult['OK']:
      self.log.verbose('Created debug script %s for Step %s' %(debugResult['Value'],self.stepNumber))
    
    if os.path.exists(self.defaultName): os.remove(self.defaultName)
    
    if os.path.exists(scriptName): os.remove(scriptName)

    if os.path.exists(self.errorLogFile): os.remove(self.errorLogFile)  

    result = shellCall(120,finalCommand,env=projectEnvironment,callbackFunction=self.redirectLogOutput)
    status = result['Value'][0]
    self.log.info("Status after the ErrorLogging execution is %s (if non-zero this is ignored)" %(status))

    if status:
      self.log.info( "Error logging for %s %s step %s completed with errors:" %(self.applicationName,self.applicationVersion,self.stepNumber))
      self.log.info( "==================================\n StdError:\n" )
      self.log.info( self.stdError )
      self.log.info('Exiting without affecting workflow status')
      return S_OK()
    
    if not os.path.exists(self.defaultName):
      self.log.info('%s not found locally, exiting without affecting workflow status' %self.defaultName)
      return S_OK()
    
    self.log.info( "Error logging for %s %s step %s completed succesfully:" %(self.applicationName,self.applicationVersion,self.stepNumber))
    shutil.copy(self.defaultName,self.errorLogName)

    if not self.enable:
      self.log.info('Module is disabled by control flag, will not publish errors to ErrorLogging')
      return S_OK('Module is disabled by control flag')    

    #TODO - report to error logging service when suitable method is available
    return S_OK()

  #############################################################################
  def redirectLogOutput(self, fd, message):
    sys.stdout.flush()
    if message:
      if self.errorLogFile:
        log = open(self.errorLogFile,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Error Log file not defined")
      if fd == 1:
        self.stdError += message

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#