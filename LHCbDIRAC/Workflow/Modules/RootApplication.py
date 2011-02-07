########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Modules/RootApplication.py $
########################################################################

""" Root Application Class """

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.DISET.RPCClient import RPCClient

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import MySiteRoot
from LHCbDIRAC.Core.Utilities.ProductionEnvironment         import getProjectEnvironment,addCommandDefaults,createDebugScript

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

import string, os, sys, fnmatch, re

class RootApplication(object):

  #############################################################################
  def __init__(self):

    self.enable = True
    self.version = __RCSID__
    self.debug = True
    self.log = gLogger.getSubLogger( "RootApplication" )
    self.result = S_ERROR()
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.logFile = ''
    self.rootVersion = ''
    self.rootScript = ''
    self.rootType = ''
    self.arguments = ''
    self.systemConfig = ''
    self.rootSection = '/Operations/SoftwareDistribution/LHCbRoot'

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
    else:
      self.log.warn('No SystemConfig defined')

    if self.step_commons.has_key('rootScript'):
      self.rootScript = self.step_commons['rootScript']
    else:
      self.log.warn('No rootScript defined')

    if self.step_commons.has_key('rootVersion'):
      self.rootVersion = self.step_commons['rootVersion']
    else:
      self.log.warn('No rootVersion defined')

    if self.step_commons.has_key('rootType'):
      self.rootType = self.step_commons['rootType']
    else:
      self.log.warn('No rootType defined')

    if self.step_commons.has_key('arguments'):
      self.arguments = self.step_commons['arguments']
      tmp=[]
      for argument in self.arguments:
        if argument:
          tmp.append(argument)
      self.arguments = tmp
    else:
      self.log.warn('No arguments specified')

    print self.arguments

    if self.step_commons.has_key('logFile'):
      self.logFile = self.step_commons['logFile']
    else:
      self.log.warn('No logFile specified')

    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the RootApplication module.
    """
    self.log.info('Initializing '+self.version)
    self.resolveInputVariables()

    self.result = S_OK()
    if not self.rootVersion:
      self.result = S_ERROR( 'No Root Version defined' )
    elif not self.systemConfig:
      self.result = S_ERROR( 'No system configuration selected' )
    elif not self.rootScript:
      self.result = S_ERROR('No script defined')
    elif not self.logFile:
      self.logFile = '%s.log' %self.rootScript
    elif not self.rootType.lower() in ('c', 'py', 'bin', 'exe'):
      self.result = S_ERROR('Wrong root type defined')

    if not self.result['OK']:
      return self.result

    self.__report( 'Initializing RootApplication module' )

    self.log.debug( self.version )
    self.log.info( "Executing application Root %s" % ( self.rootVersion ) )
    self.log.info( "Platform for job is %s" % ( self.systemConfig ) )

    result = gConfig.getOption("/".join([self.rootSection,self.rootVersion]))
    if not result['OK']:
      return self._reportError('Could not contact DIRAC Configuration Service for application for root ',__name__,**kwargs)          

    application,version = result['Value'].split('.')

    self.log.info( 'Application Found:', result['Value'] )

    #Now obtain the project environment for execution
    result = getProjectEnvironment(self.systemConfig,application,version)
    if not result['OK']:
      self.log.error('Could not obtain project environment with result: %s' %(result))
      return result # this will distinguish between LbLogin / SetupProject / actual application failures
    
    projectEnvironment = result['Value']

    if not os.path.exists(self.rootScript):
      self.log.info( 'rootScript not Found' )
      return S_ERROR( 'rootScript not Found' )

    if self.rootType.lower() == 'c':
      rootCmd = ['root']
      rootCmd.append('-b')
      rootCmd.append('-f')
      if self.arguments:
        escapedArgs=[]
        for arg in self.arguments:
          if type(arg)==type(' '):
            escapedArgs.append('\'"%s"\'' %(arg))
          else:
            escapedArgs.append('%s' %(arg))

        macroArgs = '%s\(%s\)' %(self.rootScript,string.join(escapedArgs,','))
        rootCmd.append(macroArgs)
      else:
        rootCmd.append(self.rootScript)

    elif self.rootType.lower() == 'py':

      rootCmd = ['python']
      rootCmd.append(self.rootScript)
      if self.arguments:
        rootCmd+=self.arguments

    elif self.rootType.lower() in ('bin','exe'):
      rootCmd = [os.path.abspath(self.rootScript)]
      if self.arguments:
        rootCmd+=self.arguments

    if os.path.exists(self.logFile):
      os.remove(self.logFile)

    self.log.info( 'Running:', ' '.join(rootCmd)  )
    self.__report('Running ROOT %s' %(self.rootVersion))

    ret = shellCall(0,' '.join(rootCmd),env=projectEnvironment,callbackFunction=self.redirectLogOutput)
    if not ret['OK']:
      self.log.warn('Error during: %s ' %rootCmd)
      self.log.warn(ret)
      self.result = ret
      return self.result

    resultTuple = ret['Value']
    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after %s execution is %s" %(self.rootScript,str(status)) )
    failed = False
    if status != 0:
      self.log.info( "%s execution completed with non-zero status:" % self.rootScript )
      failed = True
    elif len(stdError) > 0:
      self.log.info( "%s execution completed with application warning:" % self.rootScript )
      self.log.info(stdError)
    else:
      self.log.info( "%s execution completed succesfully:" % self.rootScript )

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( stdError )
      self.__report('%s Exited With Status %s' %(self.rootScript,status))
      self.result = S_ERROR("Script execution completed with errors")
      return self.result

    # Return OK assuming that subsequent module will spot problems
    self.__report('%s (Root %s) Successful' %(self.rootScript,self.rootVersion))
    self.result = S_OK()
    return self.result

  #############################################################################
  def redirectLogOutput(self, fd, message):

    print message
    sys.stdout.flush()
    if message:
      if self.logFile:
        log = open(self.logFile,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")

  #############################################################################
  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'RootApplication'))
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate',timeout=120)
    jobStatus = jobReport.setJobApplicationStatus(int(self.jobID),status,'RootApplication')
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def getPythonFromRoot(self,rootsys):
    """Locate the external python version that root was built with.
    """
    includedir = os.path.join(rootsys,'include')
    pythondir = ''
    for fname in os.listdir(includedir):
      if fnmatch.fnmatch(fname, '*[cC]onfig*'):
        f = file(os.path.join(includedir,fname))
        for l in f:
          i = l.find('PYTHONDIR')
          if not i==-1:
            pythondir = l[i:].split()[0].split('=')[1]

    if not pythondir:
      return S_ERROR('Root python version not found')
    pythondir = pythondir.split('/lcg/external/')[1]
    extdir = includedir.split('/lcg/external/')[0]
    pythondir = os.path.join(extdir,'lcg','external',pythondir)
    if not os.path.exists(pythondir):
      return S_ERROR('External python %s not found' %(pythondir))
    return S_OK(pythondir)


#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
