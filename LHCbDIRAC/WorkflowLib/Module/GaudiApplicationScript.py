########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Module/GaudiApplicationScript.py,v 1.1 2008/06/06 15:53:18 paterson Exp $
# File :   GaudiApplicationScript.py
# Author : Stuart Paterson
########################################################################

""" Gaudi Application Script Class

    This allows the execution of a simple python script in a given LHCb project environment,
    e.g. python <script> <arguments>. GaudiPython / Bender scripts can be executed very simply
    in this way.

    To make use of this module the LHCbJob method setApplicationScript can be called by users.
"""

__RCSID__ = "$Id: GaudiApplicationScript.py,v 1.1 2008/06/06 15:53:18 paterson Exp $"

from DIRAC.Core.Utilities                                import systemCall
from DIRAC.Core.Utilities                                import shellCall
from DIRAC.Core.Utilities                                import ldLibraryPath
from DIRAC.Core.Utilities                                import Source
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig, platformTuple

from WorkflowLib.Utilities.CombinedSoftwareInstallation  import SharedArea, LocalArea, CheckApplication

import shutil, re, string, os, sys

class GaudiApplicationScript(object):

  #############################################################################
  def __init__(self):
    self.enable = True
    self.version = __RCSID__
    self.debug = True
    self.log = gLogger.getSubLogger( "GaudiApplicationScript" )
    self.inputDataType = 'MDF'
    self.result = S_ERROR()
    self.logFile = None
    self.InputData = '' # from the (JDL WMS approach)
    self.poolXMLCatalog = 'pool_xml_catalog.xml'
    self.ExtraArguments = ''
    self.appName = ''
    self.appVersion = ''
    self.script = None
    self.arguments = ''
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

  #############################################################################
  def execute(self):

    self.result = S_OK()
    if not self.appName or not self.appVersion:
      self.result = S_ERROR( 'No Gaudi Application defined' )
    elif not self.systemConfig:
      self.result = S_ERROR( 'No LHCb platform selected' )
    elif not self.script:
      self.result = S_ERROR('No script defined')
    elif not self.logfile:
      self.logfile = '%s.log' %self.script

    if not self.result['OK']:
      return self.result

    self.__report( 'Initializing GaudiApplicationScript module' )

    self.cwd  = os.getcwd()
    self.root = gConfig.getValue( '/LocalSite/Root', self.cwd )

    self.log.debug( self.version )
    self.log.info( "Executing application %s %s" % ( self.appName, self.appVersion ) )
    self.log.info( "Platform for job is %s" % ( self.systemConfig ) )
    self.log.info( "Root directory for job is %s" % ( self.root ) )

    sharedArea = SharedArea()
    localArea  = LocalArea()

    # 1. Check if Application is available in Shared Area
    appCmd = CheckApplication( ( self.appName, self.appVersion ), self.systemConfig, sharedArea )
    if appCmd:
      mySiteRoot = sharedArea
    else:
      # 2. If not, check if available in Local Area
      appCmd = CheckApplication( ( self.appName, self.appVersion ), self.systemConfig, localArea )
      if appCmd:
        mySiteRoot = localArea
      else:
        self.log.warn( 'Application not Found' )
        self.__report( 'Application Not Found' )
        self.result = S_ERROR( 'Application not Found' )

    if not self.result['OK']:
      return self.result

    self.__report( 'Application Found' )
    self.log.info( 'Application Found:', appCmd )
    appRoot = os.path.dirname(os.path.dirname( appCmd ))

    cmtEnv = dict(os.environ)
    gaudiEnv = {}

    cmtEnv['MYSITEROOT'] = mySiteRoot
    cmtEnv['CMTCONFIG']  = self.systemConfig

    extCMT       = os.path.join( mySiteRoot, 'scripts', 'ExtCMT' )
    setupProject = os.path.join( mySiteRoot, 'scripts', 'SetupProject' )
    setupProject = [setupProject]
    setupProject.append( '--ignore-missing' )
    setupProject.append( self.appName )
    setupProject.append( self.appVersion )
    setupProject.append( 'gfal CASTOR dcache_client lfc oracle' )

    timeout = 300

    # Run ExtCMT
    ret = Source( timeout, [extCMT], cmtEnv )
    if ret['OK']:
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      setupProjectEnv = ret['outputEnv']
    else:
      self.log.error( ret['Message'])
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      self.result = ret
      return self.result

    # Run SetupProject
    ret = Source( timeout, setupProject, setupProjectEnv )
    if ret['OK']:
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      gaudiEnv = ret['outputEnv']
    else:
      self.log.error( ret['Message'])
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      self.result = ret
      return self.result

    # Now link all libraries in a single directory
    appDir = os.path.join( self.cwd, '%s_%s' % ( self.appName, self.appVersion ))
    if os.path.exists( appDir ):
      import shutil
      shutil.rmtree( appDir )
    # add shipped libraries if available
    # extraLibs = os.path.join( mySiteRoot, self.systemConfig )
    #if os.path.exists( extraLibs ):
    #  gaudiEnv['LD_LIBRARY_PATH'] += ':%s' % extraLibs
    #  self.log.info( 'Adding %s to LD_LIBRARY_PATH' % extraLibs )
    # Add compat libs
    compatLib = os.path.join( self.root, self.systemConfig, 'compat' )
    if os.path.exists(compatLib):
      gaudiEnv['LD_LIBRARY_PATH'] += ':%s' % compatLib
    gaudiEnv['LD_LIBRARY_PATH'] = ldLibraryPath.unify( gaudiEnv['LD_LIBRARY_PATH'], appDir )

    f = open( 'localEnv.log', 'w' )
    for k in gaudiEnv:
      v = gaudiEnv[k]
      f.write( '%s=%s\n' % ( k,v ) )
    f.close()

    gaudiCmd = []
    if re.search('.py$',self.script):
      gaudiCmd.append('python')
      gaudiCmd.append(self.script)
      gaudiCmd.append(self.arguments)
    else:
      gaudiCmd.append(self.script)
      gaudiCmd.append(self.arguments)

    if not self.poolXMLCatalog=='pool_xml_catalog.xml':
      if not os.path.exists(self.poolXMLCatalog):
        self.log.info('Creating requested POOL XML Catalog: %s' %(self.poolXMLCatalog))
        shutil.copy('pool_xml_catalog.xml',self.poolXMLCatalog)

    self.log.info('POOL XML Catalog file is %s' %(self.poolXMLCatalog))

    if os.path.exists(self.logfile): os.remove(self.logfile)
    self.__report('%s %s' %(self.appName,self.appVersion))
    self.writeGaudiRun(gaudiCmd, gaudiEnv)
    self.log.info( 'Running:', ' '.join(gaudiCmd)  )
    ret = systemCall(0,gaudiCmd,env=gaudiEnv,callbackFunction=self.redirectLogOutput)

    if not ret['OK']:
      self.log.error(gaudiCmd)
      self.log.error(ret)
      self.result = S_ERROR()
      return self.result

      self.result = ret
      return self.result

    resultTuple = ret['Value']

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after %s execution is %s" %(self.script,str(status)) )

    failed = False
    if status > 0:
      self.log.info( "%s execution completed with non-zero status:" % self.script )
      failed = True
    elif len(stdError) > 0:
      self.log.info( "%s execution completed with application warning:" % self.script )
      self.log.info(stdError)
    else:
      self.log.info( "%s execution completed succesfully:" % self.script )

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( stdError )
      self.__report('%s Exited With Status %s' %(self.script,status))
      self.result = S_ERROR("Script execution completed with errors")
      return self.result

    # Return OK assuming that subsequent CheckLogFile will spot problems
    self.__report('%s (%s %s) Successful' %(self.script,self.appName,self.appVersion))
    self.result = S_OK()
    return self.result

  #############################################################################
  def writeGaudiRun( self, gaudiCmd, gaudiEnv, shell='/bin/bash'):
    """
     Write a shell script that can be used to run the application
     with the same environment as in GaudiApplication.
     This script is not used internally, but can be used "a posteriori" for
     debugging purposes
    """
    if shell.find('csh'):
      ext = 'csh'
      environ = []
      for var in gaudiEnv:
        environ.append('setenv %s "%s"' % (var, gaudiEnv[var]) )
    else:
      ext = 'sh'
      for var in gaudiEnv:
        environ.append('export %s="%s"' % (var, gaudiEnv[var]) )

    scriptName = '%sRun.%s' % ( self.appName, ext )
    script = open( scriptName, 'w' )
    script.write( """#! %s
#
# This is a debug script to run %s %s interactively
#
%s
#
%s | tee %s
#
exit $?
#
""" % (shell, self.appName, self.appVersion,
        '\n'.join(environ),
        ' '.join(gaudiCmd),
        self.logfile ) )

    script.close()

  #############################################################################
  def redirectLogOutput(self, fd, message):
    print message
    sys.stdout.flush()
    if message:
      if self.logfile:
        log = open(self.logfile,'a')
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

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'GaudiApplication'))
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobStatus = jobReport.setJobApplicationStatus(int(self.jobID),status,'GaudiApplication')
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#