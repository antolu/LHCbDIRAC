########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/WorkflowLib/Module/GaudiApplicationScript.py $
# File :   GaudiApplicationScript.py
# Author : Stuart Paterson
########################################################################

""" Gaudi Application Script Class

    This allows the execution of a simple python script in a given LHCb project environment,
    e.g. python <script> <arguments>. GaudiPython / Bender scripts can be executed very simply
    in this way.

    To make use of this module the LHCbJob method setApplicationScript can be called by users.
"""

__RCSID__ = "$Id: GaudiApplicationScript.py 18064 2009-11-05 19:40:01Z acasajus $"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities                                import ldLibraryPath
from DIRAC.Core.Utilities                                import Source
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient

from LHCbDIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import MySiteRoot, CheckApplication
from LHCbDIRAC.Workflow.Modules.ModuleBase                        import ModuleBase

from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig, platformTuple

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
    self.inputDataType = 'DATA'
    self.inputData = '' # to be resolved, check at the step level
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

    if self.step_commons.has_key('inputDataType'):
      self.inputDataType = self.step_commons['inputDataType']

    if self.step_commons.has_key('inputData'):
      self.inputData = self.step_commons['inputData']

    if self.step_commons.has_key('poolXMLCatName'):
      self.poolXMLCatName = self.step_commons['poolXMLCatName']
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the module.
    """
    self.log.info('Initializing '+self.version)
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
      self.applicationLog = '%s.log' %self.script

    if not self.result['OK']:
      return self.result

    #self.__report( 'Initializing GaudiApplicationScript module' )
    self.cwd  = os.getcwd()
    self.root = gConfig.getValue( '/LocalSite/Root', self.cwd )

    self.log.debug( self.version )
    self.log.info( "Executing application %s %s" % ( self.applicationName, self.applicationVersion ) )
    self.log.info( "System configuration for job is %s" % ( self.systemConfig ) )
    self.log.info( "Root directory for job is %s" % ( self.root ) )

    sharedArea = MySiteRoot()
    appRoot = CheckApplication( ( self.applicationName, self.applicationVersion ), self.systemConfig, sharedArea )
    if appRoot:
      mySiteRoot = sharedArea
    else:
      self.log.error( 'Application Not found' )
      self.setApplicationStatus( 'Application Not Found' )
      self.result = S_ERROR( 'Application Not Found' )

    if not self.result['OK']:
      return self.result

    localArea = sharedArea
    if re.search(':',sharedArea):
      localArea = string.split(sharedArea,':')[0]
    self.log.info('Setting local software area to %s' %localArea)

    #self.__report( 'Application Found' )
    self.log.info( 'Application Root Found:', appRoot )

    cmtEnv = dict(os.environ)
    if 'CMTPROJECTPATH' in cmtEnv:
      print 'CMTPROJECTPATH',cmtEnv['CMTPROJECTPATH']
      del cmtEnv['CMTPROJECTPATH']

    gaudiEnv = {}

    cmtEnv['MYSITEROOT'] = mySiteRoot
    cmtEnv['CMTCONFIG']  = self.systemConfig

    extCMT       = os.path.join( localArea, 'LbLogin' )
    setupProject = '%s/%s' %(os.path.dirname(os.path.realpath('%s.sh' %extCMT)),'SetupProject')

    setupProject = [setupProject]
    setupProject.append( '--ignore-missing' )
    setupProject.append( self.applicationName )
    setupProject.append( self.applicationVersion )

    externals = ''
    if gConfig.getOption( '/Operations/ExternalsPolicy/%s' % DIRAC.siteName() )['OK']:
      externals = gConfig.getValue( '/Operations/ExternalsPolicy/%s' % DIRAC.siteName(), [] )
      externals = string.join( externals,' ')
      self.log.info( 'Found externals policy for %s = %s' % ( DIRAC.siteName(), externals ) )
    else:
      externals = gConfig.getValue( '/Operations/ExternalsPolicy/Default', [] )
      externals = string.join( externals, ' ' )
      self.log.info( 'Using default externals policy for %s = %s' % ( DIRAC.siteName(), externals ) )

    setupProject.append( externals )
    timeout = 600

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
    appDir = os.path.join( self.cwd, '%s_%s' % ( self.applicationName, self.applicationVersion ))
    if os.path.exists( appDir ):
      self.log.verbose('Using existing %s_%s directory' % ( self.applicationName, self.applicationVersion ))
#      import shutil
#      shutil.rmtree( appDir )
    # add shipped libraries if available
    # extraLibs = os.path.join( mySiteRoot, self.systemConfig )
    #if os.path.exists( extraLibs ):
    #  gaudiEnv['LD_LIBRARY_PATH'] += ':%s' % extraLibs
    #  self.log.info( 'Adding %s to LD_LIBRARY_PATH' % extraLibs )
    # Add compat libs

    localPath = os.path.abspath('.')
    if os.path.exists('%s/lib' %localPath):
      self.log.info('Found local lib directory, prepending to LD_LIBRARY_PATH')
      origLD = '' #just in case LD_LIBRARY_PATH is not defined
      if gaudiEnv.has_key('LD_LIBRARY_PATH'):
        origLD = gaudiEnv['LD_LIBRARY_PATH']
        gaudiEnv['LD_LIBRARY_PATH']='%s/lib:%s' %(localPath,origLD)
      else:
        gaudiEnv['LD_LIBRARY_PATH']='%s/lib' %localPath

    compatLib = os.path.join( self.root, self.systemConfig, 'compat' )
    if os.path.exists(compatLib):
      gaudiEnv['LD_LIBRARY_PATH'] += ':%s' % compatLib

    f = open( 'localEnv.log', 'w' )
    for k in gaudiEnv:
      v = gaudiEnv[k]
      f.write( '%s="%s"\n' % ( k,v ) )
    f.close()

    gaudiCmd = []
    if re.search('.py$',self.script):
      gaudiCmd.append('python')
      gaudiCmd.append(os.path.basename(self.script))
      gaudiCmd.append(self.arguments)
    else:
      gaudiCmd.append(os.path.basename(self.script))
      gaudiCmd.append(self.arguments)

    if not self.poolXMLCatName=='pool_xml_catalog.xml':
      if not os.path.exists(self.poolXMLCatName):
        self.log.info('Creating requested POOL XML Catalog: %s' %(self.poolXMLCatName))
        shutil.copy('pool_xml_catalog.xml',self.poolXMLCatName)

    self.log.info('POOL XML Catalog file is %s' %(self.poolXMLCatName))

    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)
    #self.__report('%s %s' %(self.applicationName,self.applicationVersion))
    self.writeGaudiRun(gaudiCmd, gaudiEnv)
    self.stdError = ''
    ret = shellCall(0,'which python',env=gaudiEnv,callbackFunction=self.redirectLogOutput)
    self.log.info( 'Running:', ' '.join(gaudiCmd)  )
    ret = shellCall(0,' '.join(gaudiCmd),env=gaudiEnv,callbackFunction=self.redirectLogOutput)

    if not ret['OK']:
      self.log.error(gaudiCmd)
      self.log.error(ret)
      self.result = S_ERROR()
      return self.result

      self.result = ret
      return self.result

    resultTuple = ret['Value']

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
      #self.__report('%s Exited With Status %s' %(os.path.basename(self.script),status))
      return S_ERROR('%s Exited With Status %s' %(os.path.basename(self.script),status))

    # Return OK assuming that subsequent CheckLogFile will spot problems
    self.__report('%s (%s %s) Successful' %(os.path.basename(self.script),self.applicationName,self.applicationVersion))
    #Above can't be removed as it is the last notification for user jobs
    return S_OK('%s (%s %s) Successful' %(os.path.basename(self.script),self.applicationName,self.applicationVersion))

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

    scriptName = '%sRun.%s' % ( self.applicationName, ext )
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
""" % (shell, self.applicationName, self.applicationVersion,
        '\n'.join(environ),
        ' '.join(gaudiCmd),
        self.applicationLog ) )

    script.close()

  #############################################################################
  def redirectLogOutput(self, fd, message):
    print message
    sys.stdout.flush()
    if message:
      if self.applicationLog:
        log = open(self.applicationLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")
      if fd == 1:
        self.stdError += message
      

  #############################################################################
  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'GaudiApplication'))
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate',timeout=120)
    jobStatus = jobReport.setJobApplicationStatus(int(self.jobID),status,'GaudiApplication')
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#