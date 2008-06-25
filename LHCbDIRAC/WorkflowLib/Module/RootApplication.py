""" Root Application Class """

__RCSID__ = "$Id: RootApplication.py,v 1.1 2008/06/25 08:40:45 roma Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities import systemCall

from WorkflowLib.Utilities.CombinedSoftwareInstallation  import SharedArea, LocalArea, CheckApplication

import os,sys

class RootApplication(object):

  def __init__(self):
  
    self.enable = True	
    self.version = __RCSID__
    self.debug = True
    self.log = gLogger.getSubLogger( "RootApplication" )
    self.result = S_ERROR()
    self.logFile = None
    self.rootversion = ''
    self.rootscript = None
    self.roottype = 'C'
    self.arguments = ''
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
    

  def execute(self):
    self.log.info( "Executing RootApplication " )

    self.result = S_OK()
    if not self.rootversion:
      self.result = S_ERROR( 'No Root Version defined' )
    elif not self.systemConfig:
      self.result = S_ERROR( 'No platform selected' )
    elif not self.rootscript:
      self.result = S_ERROR('No script defined')
    elif not self.logfile:
      self.logfile = '%s.log' %self.rootscript
    elif not self.roottype.lower() in ('c', 'py', 'bin', 'exe'):
      self.result = S_ERROR('Wrong root type defined')

    if not self.result['OK']:
      return self.result

    self.__report( 'Initializing RootApplication module' )

    self.cwd  = os.getcwd()
    self.root = gConfig.getValue( '/LocalSite/Root', self.cwd )

    self.log.debug( self.version )
    self.log.info( "Executing application Root %s" % ( self.rootversion ) )
    self.log.info( "Platform for job is %s" % ( self.systemConfig ) )
    self.log.info( "Root directory for job is %s" % ( self.root ) )
    
    if self.jobID:
      return self.result	#??? WHEREFORE: Don't know how to find ROOT on WN
    sharedArea = SharedArea()
    rootdir = os.path.join(sharedArea,"lcg/external/root", self.rootversion, self.systemConfig, "root")
    if not os.path.exists(rootdir):
      self.log.warn( 'Application not Found' )
      self.__report( 'Application Not Found' )
      return S_ERROR( 'Application not Found' )

    self.log.info( 'Application Found:', rootdir )
    self.__report( 'Application Found' )

    rootEnv = dict(os.environ)
    rootEnv['ROOTSYS']=rootdir
    rootEnv['PATH'] += ":%s" %(os.path.join(rootdir, 'bin'))

    if rootEnv.has_key('LD_LIBRARY_PATH'):
      rootEnv['LD_LIBRARY_PATH'] += ":%s"%(os.path.join(rootdir, 'lib'))
    else:
      rootEnv['LD_LIBRARY_PATH'] = os.path.join(rootdir, 'lib')

    if os.path.exists('lib'):
      rootEnv['LD_LIBRARY_PATH'] += ":%s"%(os.path.abspath('lib'))
    
    if not os.path.exists(self.rootscript):
      self.log.info( 'Rootscript not Found' )
      return S_ERROR( 'Rootscript not Found' )


    if self.roottype.lower() == 'c':
      rootCmd = os.path.join(rootdir, 'bin/root')
      rootCmd = [rootCmd]
      rootCmd.append('-b')    
      rootCmd.append('-f')    
      rootCmd.append(self.rootscript)
      rootCmd.append(self.arguments)
      
    elif self.roottype.lower() == 'py':

      if rootEnv.has_key('PYTHONPATH'):
        rootEnv['PYTHONPATH'] += ":%s"%(os.path.join(rootdir, 'lib'))
      else:
        rootEnv['PYTHONPATH'] = os.path.join(rootdir, 'lib')
      
      pythondir = getPythonFromRoot(rootdir)
      rootEnv['LD_LIBRARY_PATH'] += ":%s"%(os.path.join(pythondir, 'lib'))
      pythonbin = os.path.join(pythondir, 'bin', 'python')

      rootCmd = [pythonbin]
      rootCmd.append(self.rootscript)
      rootCmd.append(self.arguments)
      
    elif self.roottype.lower() in ('bin','exe'):
      rootCmd = [os.path.abspath(self.rootscript)]
      rootCmd.append(self.arguments)
   
    if os.path.exists(self.logfile):
      os.remove(self.logfile)

    self.log.info( 'Running:', ' '.join(rootCmd)  )
    self.__report(' '.join(rootCmd))

    ret = systemCall(0,rootCmd,env=rootEnv,callbackFunction=self.redirectLogOutput)

    if not ret['OK']:
      self.log.error(rootCmd)
      self.log.error(ret)
      self.result = ret
      return self.result

    resultTuple = ret['Value']
    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after %s execution is %s" %(self.rootscript,str(status)) )
    failed = False
    if status > 0:
      self.log.info( "%s execution completed with non-zero status:" % self.rootscript )
      failed = True
    elif len(stdError) > 0:
      self.log.info( "%s execution completed with application warning:" % self.rootscript )
      self.log.info(stdError)
    else:
      self.log.info( "%s execution completed succesfully:" % self.rootscript )

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( stdError )
      self.__report('%s Exited With Status %s' %(self.script,status))
      self.result = S_ERROR("Script execution completed with errors")
      return self.result

    # Return OK assuming that subsequent CheckLogFile will spot problems
    self.__report('%s (Root %s) Successful' %(self.rootscript,self.rootversion))
    self.result = S_OK()
    return self.result
        
  def redirectLogOutput(self, fd, message):

    print message
    sys.stdout.flush()
    if message:    #??? What is means
      if self.logfile:
        log = open(self.logfile,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")

  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    from DIRAC.Core.DISET.RPCClient import RPCClient

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'GaudiApplication')) 
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobStatus = jobReport.setJobApplicationStatus(int(self.jobID),status,'GaudiApplication')    #???
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

def getPythonFromRoot(rootsys):

  import fnmatch, os
  includedir = os.path.join(rootsys,'include')

  for fname in os.listdir(includedir):
    if fnmatch.fnmatch(fname, '*[cC]onfig*'):
      f = file(os.path.join(includedir,fname))
      for l in f:
        i = l.find('PYTHONDIR')
        if not i==-1:
          return l[i:].split()[0].split('=')[1]
