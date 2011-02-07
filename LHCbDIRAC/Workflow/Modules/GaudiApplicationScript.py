########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Modules/GaudiApplicationScript.py $
# File :   GaudiApplicationScript.py
# Author : Stuart Paterson
########################################################################

""" Gaudi Application Script Class

    This allows the execution of a simple python script in a given LHCb project environment,
    e.g. python <script> <arguments>. GaudiPython / Bender scripts can be executed very simply
    in this way.

    To make use of this module the LHCbJob method setApplicationScript can be called by users.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Os                                import sourceEnv
from DIRAC.Resources.Catalog.PoolXMLCatalog                 import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                             import RPCClient

from LHCbDIRAC.Core.Utilities.ProductionEnvironment         import getProjectEnvironment,addCommandDefaults,createDebugScript
from LHCbDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC import S_OK, S_ERROR, gLogger, gConfig, platformTuple, shellCall

import DIRAC

import shutil, re, string, os, sys

class GaudiApplicationScript(ModuleBase):

  #############################################################################
  def __init__(self):
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "GaudiApplicationScript" )
    self.result = S_ERROR()
    self.jobID = None
    self.root = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Set defaults for all workflow parameters here
    self.script = None
    self.arguments = ''
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationName = ''
    self.applicationVersion = ''
    self.systemConfig = ''
    self.poolXMLCatName = 'pool_xml_catalog.xml'

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    print self.step_commons
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
    else:
      self.log.warn('No SystemConfig defined')

    if self.step_commons.has_key('applicationName'):
      self.applicationName = self.step_commons['applicationName']
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']
    else:
      self.log.warn('No applicationName defined')

    if self.step_commons.has_key('script'):
      self.script = self.step_commons['script']
      print self.script
    else:
      self.log.warn('No script defined')

    print self.step_commons.has_key('script')
    print self.step_commons['script']

    if self.step_commons.has_key('arguments'):
      self.arguments = self.step_commons['arguments']

    if self.step_commons.has_key('poolXMLCatName'):
      self.poolXMLCatName = self.step_commons['poolXMLCatName']
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the module.
    """
    self.log.info('Initializing %s' %(self.version))
    self.log.verbose('Step commons is:\n%s' %self.step_commons)
    self.log.verbose('Workflow commons is:\n%s' %self.workflow_commons)
    self.resolveInputVariables()

    self.result = S_OK()
    if not self.applicationName or not self.applicationVersion:
      self.result = S_ERROR( 'No Gaudi Application defined' )
    elif not self.systemConfig:
      self.result = S_ERROR( 'No LHCb system configuration selected' )
    elif not self.script:
      self.result = S_ERROR('No script defined')
    elif not self.applicationLog:
      self.applicationLog = '%s.log' %(os.path.basename(self.script))

    if not self.result['OK']:
      return self.result

    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    self.log.info( "Executing application %s %s for system configuration %s" %(self.applicationName,self.applicationVersion,self.systemConfig))
    self.log.verbose("/LocalSite/Root directory for job is %s" %(self.root))

    #Now obtain the project environment for execution
    result = getProjectEnvironment(self.systemConfig,self.applicationName,self.applicationVersion,poolXMLCatalogName=self.poolXMLCatName)
    if not result['OK']:
      self.log.error('Could not obtain project environment with result: %s' %(result))
      return result # this will distinguish between LbLogin / SetupProject / actual application failures

    projectEnvironment = result['Value']    
    
    gaudiCmd = []
    if re.search('.py$',self.script):
      gaudiCmd.append('python')
      gaudiCmd.append(os.path.basename(self.script))
      gaudiCmd.append(self.arguments)
    else:
      gaudiCmd.append(os.path.basename(self.script))
      gaudiCmd.append(self.arguments)

    command = ' '.join(gaudiCmd)
    print 'Command = %s' %(command)  #Really print here as this is useful to see

    #Set some parameter names
    dumpEnvName = 'Environment_Dump_%s_%s_Step%s.log' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER)
    scriptName = '%s_%s_Run_%s.sh' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER)
    coreDumpName = '%s_Step%s' %(self.applicationName,self.STEP_NUMBER)
    
    #Wrap final execution command with defaults
    finalCommand = addCommandDefaults(command,envDump=dumpEnvName,coreDumpLog=coreDumpName)['Value'] #should always be S_OK()

    #Create debug shell script to reproduce the application execution
    debugResult = createDebugScript(scriptName,command,env=projectEnvironment,envLogFile=dumpEnvName,coreDumpLog=coreDumpName) #will add command defaults internally
    if debugResult['OK']:
      self.log.verbose('Created debug script %s for Step %s' %(debugResult['Value'],self.STEP_NUMBER))

    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    self.stdError = ''    
    result = shellCall(0,finalCommand,env=projectEnvironment,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Problem Executing Application')

    resultTuple = result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after %s execution is %s" %(os.path.basename(self.script),str(status)) )
    failed = False
    if status != 0:
      self.log.info( "%s execution completed with non-zero status:" % os.path.basename(self.script) )
      failed = True
    elif len(self.stdError) > 0:
      self.log.info( "%s execution completed with application warning:" % os.path.basename(self.script) )
      self.log.info(self.stdError)
    else:
      self.log.info( "%s execution completed succesfully:" % os.path.basename(self.script) )

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      return S_ERROR('%s Exited With Status %s' %(os.path.basename(self.script),status))

    #Above can't be removed as it is the last notification for user jobs
    self.setApplicationStatus('%s (%s %s) Successful' %(os.path.basename(self.script),self.applicationName,self.applicationVersion))
    return S_OK('%s (%s %s) Successful' %(os.path.basename(self.script),self.applicationName,self.applicationVersion))
      
  #############################################################################
  def redirectLogOutput(self, fd, message):
    sys.stdout.flush()
    if message:
      if re.search('INFO Evt',message): print message
      if self.applicationLog:
        log = open(self.applicationLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")
      if fd == 1:
        self.stdError += message

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#