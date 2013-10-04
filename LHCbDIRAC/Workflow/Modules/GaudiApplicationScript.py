""" Gaudi Application Script Class

    This allows the execution of a simple python script in a given LHCb project environment,
    e.g. python <script> <arguments>. GaudiPython / Bender scripts can be executed very simply
    in this way.

    To make use of this module the LHCbJob method setApplicationScript can be called by users.
"""

__RCSID__ = "$Id$"

import re, os, sys

from DIRAC import S_OK, S_ERROR, gLogger, gConfig, shellCall
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectEnvironment, addCommandDefaults, createDebugScript
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

class GaudiApplicationScript( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, rm = None ):
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "GaudiApplicationScript" )
    super( GaudiApplicationScript, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )

    self.result = S_ERROR()

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

  def _resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """

    super( GaudiApplicationScript, self )._resolveInputVariables()
    super( GaudiApplicationScript, self )._resolveInputStep()

    if self.step_commons.has_key( 'script' ):
      self.script = self.step_commons['script']
    else:
      self.log.warn( 'No script defined' )

    if self.step_commons.has_key( 'arguments' ):
      self.arguments = self.step_commons['arguments']

    if self.step_commons.has_key( 'poolXMLCatName' ):
      self.poolXMLCatName = self.step_commons['poolXMLCatName']

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None,
               projectEnvironment = None ):
    """The main execution method of the module.
    """

    try:

      super( GaudiApplicationScript, self ).execute( self.version,
                                                     production_id, prod_job_id, wms_job_id,
                                                     workflowStatus, stepStatus,
                                                     wf_commons, step_commons,
                                                     step_number, step_id )

      self._resolveInputVariables()

      self.result = S_OK()
      if not self.applicationName or not self.applicationVersion:
        self.result = S_ERROR( 'No Gaudi Application defined' )
      elif not self.systemConfig:
        self.result = S_ERROR( 'No LHCb system configuration selected' )
      elif not self.script:
        self.result = S_ERROR( 'No script defined' )
      elif not self.applicationLog:
        self.applicationLog = '%s.log' % ( os.path.basename( self.script ) )

      if not self.result['OK']:
        return self.result

      self.log.info( "Executing application %s %s for system configuration %s" % ( self.applicationName,
                                                                                   self.applicationVersion,
                                                                                   self.systemConfig ) )
      self.log.verbose( "/LocalSite/Root directory for job is %s" % ( gConfig.getValue( '/LocalSite/Root',
                                                                                        os.getcwd() ) ) )

      #Now obtain the project environment for execution
      if not projectEnvironment:
        result = getProjectEnvironment( self.systemConfig,
                                        self.applicationName,
                                        self.applicationVersion,
                                        poolXMLCatalogName = self.poolXMLCatName )
        if not result['OK']:
          self.log.error( 'Could not obtain project environment with result: %s' % ( result ) )
          return result # this will distinguish between LbLogin / SetupProject / actual application failures

        projectEnvironment = result['Value']

      gaudiCmd = []
      if re.search( '.py$', self.script ):
        gaudiCmd.append( 'python' )
        gaudiCmd.append( os.path.basename( self.script ) )
        gaudiCmd.append( self.arguments )
      else:
        gaudiCmd.append( os.path.basename( self.script ) )
        gaudiCmd.append( self.arguments )

      command = ' '.join( gaudiCmd )
      print 'Command = %s' % ( command )  #Really print here as this is useful to see

      #Set some parameter names
      dumpEnvName = 'Environment_Dump_%s_%s_Step%s.log' % ( self.applicationName,
                                                            self.applicationVersion,
                                                            self.step_number )
      scriptName = '%s_%s_Run_%s.sh' % ( self.applicationName, self.applicationVersion, self.step_number )
      coreDumpName = '%s_Step%s' % ( self.applicationName, self.step_number )

      #Wrap final execution command with defaults
      finalCommand = addCommandDefaults( command, envDump = dumpEnvName, coreDumpLog = coreDumpName )['Value'] #should always be S_OK()

      #Create debug shell script to reproduce the application execution
      debugResult = createDebugScript( scriptName,
                                       command,
                                       env = projectEnvironment,
                                       envLogFile = dumpEnvName,
                                       coreDumpLog = coreDumpName ) #will add command defaults internally
      if debugResult['OK']:
        self.log.verbose( 'Created debug script %s for Step %s' % ( debugResult['Value'], self.step_number ) )

      if os.path.exists( self.applicationLog ):
        os.remove( self.applicationLog )

      self.stdError = ''
      result = shellCall( 0, finalCommand,
                          env = projectEnvironment,
                          callbackFunction = self.redirectLogOutput,
                          bufferLimit = 20971520 )
      if not result['OK']:
        self.log.error( result )
        return S_ERROR( 'Problem Executing Application' )

      resultTuple = result['Value']

      status = resultTuple[0]
      # stdOutput = resultTuple[1]
      # stdError = resultTuple[2]
      self.log.info( "Status after %s execution is %s" % ( os.path.basename( self.script ), str( status ) ) )
      failed = False
      if status != 0:
        self.log.info( "%s execution completed with non-zero status:" % os.path.basename( self.script ) )
        failed = True
      elif len( self.stdError ) > 0:
        self.log.info( "%s execution completed with application warning:" % os.path.basename( self.script ) )
        self.log.info( self.stdError )
      else:
        self.log.info( "%s execution completed successfully:" % os.path.basename( self.script ) )

      if failed == True:
        self.log.error( "==================================\n StdError:\n" )
        self.log.error( self.stdError )
        return S_ERROR( '%s Exited With Status %s' % ( os.path.basename( self.script ), status ) )

      #Above can't be removed as it is the last notification for user jobs
      self.setApplicationStatus( '%s (%s %s) Successful' % ( os.path.basename( self.script ),
                                                             self.applicationName,
                                                             self.applicationVersion ) )

      return S_OK( '%s (%s %s) Successful' % ( os.path.basename( self.script ),
                                               self.applicationName,
                                               self.applicationVersion ) )

    except Exception, e:
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( GaudiApplicationScript, self ).finalize( self.version )

  #############################################################################

  def redirectLogOutput( self, fd, message ):
    sys.stdout.flush()
    if message:
      if re.search( 'INFO Evt', message ): print message
      if self.applicationLog:
        log = open( self.applicationLog, 'a' )
        log.write( message + '\n' )
        log.close()
      else:
        self.log.error( "Application Log file not defined" )
      if fd == 1:
        self.stdError += message

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
