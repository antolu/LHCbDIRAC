""" Gaudi Application module - main module: creates the environment,
    executes gaudirun with the right options
"""

__RCSID__ = "$Id$"

import re, os, sys, multiprocessing

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities.Subprocess  import shellCall

from LHCbDIRAC.Core.Utilities.ProductionOptions import getDataOptions, getModuleOptions
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectEnvironment, addCommandDefaults, createDebugScript
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Workflow.Modules.ModulesUtilities import multicoreWN


class GaudiApplication( ModuleBase ):
  """ GaudiApplication class
  """

  #############################################################################

  def __init__( self, bkClient = None, dm = None ):
    """ Usual init for LHCb workflow modules
    """

    self.log = gLogger.getSubLogger( "GaudiApplication" )
    super( GaudiApplication, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__
    self.debug = True

    self.optfile = ''
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationName = ''
    self.applicationVersion = ''
    self.runTimeProjectName = ''
    self.runTimeProjectVersion = ''
    self.inputDataType = 'MDF'
    self.stepInputData = []  # to be resolved
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.optionsFile = ''
    self.optionsLine = ''
    self.extraOptionsLine = ''
    self.extraPackages = ''
    self.applicationType = ''
    self.optionsFormat = ''
    self.histoName = ''
    self.jobType = ''
    self.stdError = ''
    self.DDDBTag = ''
    self.condDBTag = ''
    self.dqTag = ''
    self.outputFilePrefix = ''
    self.runNumber = 0
    self.TCK = ''
    self.mcTCK = ''
    self.multicoreJob = 'True'
    self.multicoreStep = 'N'
    self.processingPass = ''

  #############################################################################

  def _resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """

    super( GaudiApplication, self )._resolveInputVariables()
    super( GaudiApplication, self )._resolveInputStep()

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_id = None, step_number = None,
               projectEnvironment = None ):
    """ The main execution method of GaudiApplication.
    """

    try:

      super( GaudiApplication, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                               workflowStatus, stepStatus,
                                               wf_commons, step_commons, step_number, step_id )

      if not self._checkWFAndStepStatus():
        return S_OK()

      self._resolveInputVariables()

      self.log.info( "Executing application %s %s for CMT configuration %s" % ( self.applicationName,
                                                                                self.applicationVersion,
                                                                                self.systemConfig ) )
      self.log.verbose( "/LocalSite/Root directory for job is %s" % ( gConfig.getValue( '/LocalSite/Root',
                                                                                        os.getcwd() ) ) )

      if self.jobType.lower() == 'merge':
        self._disableWatchdogCPUCheck()

      # Resolve options files
      if self.optionsFile and not self.optionsFile == "None":
        for fileopt in self.optionsFile.split( ';' ):
          if os.path.exists( '%s/%s' % ( os.getcwd(), os.path.basename( fileopt ) ) ):
            self.optfile += ' ' + os.path.basename( fileopt )
          # Otherwise take the one from the application options directory
          elif re.search( '\$', fileopt ):
            self.log.info( 'Found options file containing environment variable: %s' % fileopt )
            self.optfile += '  %s' % ( fileopt )
          else:
            self.log.error( 'Cannot process options: "%s" not found via environment variable \
            or in local directory' % ( fileopt ) )

      self.log.info( 'Final options files: %s' % ( self.optfile ) )

      runNumberGauss = 0
      firstEventNumberGauss = 0
      if self.applicationName.lower() == "gauss" and self.production_id and self.prod_job_id:
        if self.jobType.lower() == 'user':
          eventsMax = self.numberOfEvents
        else:
          # maintaining backward compatibility
          eventsMax = self.maxNumberOfEvents if self.maxNumberOfEvents else self.numberOfEvents
        runNumberGauss = int( self.production_id ) * 100 + int( self.prod_job_id )
        firstEventNumberGauss = eventsMax * ( int( self.prod_job_id ) - 1 ) + 1

      if self.optionsLine or self.jobType.lower() == 'user':
        self.log.debug( "Won't get any step outputs (USER job)" )
        stepOutputs = []
        stepOutputTypes = []
        histogram = False
      else:
        self.log.debug( "Getting the step outputs" )
        stepOutputs, stepOutputTypes, histogram = self._determineOutputs()


      if self.optionsLine or self.jobType.lower() == 'user':
        # Prepare standard project run time options
        generatedOpts = 'gaudi_extra_options.py'
        if os.path.exists( generatedOpts ):
          os.remove( generatedOpts )
        inputDataOpts = getDataOptions( self.applicationName,
                                        self.stepInputData,
                                        self.inputDataType,
                                        self.poolXMLCatName )['Value']  # always OK
        projectOpts = getModuleOptions( self.applicationName,
                                        self.numberOfEvents,
                                        inputDataOpts,
                                        self.optionsLine,
                                        runNumberGauss,
                                        firstEventNumberGauss,
                                        self.jobType )['Value']  # always OK
        self.log.info( 'Extra options generated for %s %s step:' % ( self.applicationName, self.applicationVersion ) )
        print projectOpts  # Always useful to see in the logs (don't use gLogger as we often want to cut n' paste)
        options = open( generatedOpts, 'w' )
        options.write( projectOpts )
        options.close()

      else:
        prodConfFileName = self.createProdConfFile( stepOutputTypes, histogram, runNumberGauss, firstEventNumberGauss )


      if not projectEnvironment:
        # Now obtain the project environment for execution
        result = getProjectEnvironment( systemConfiguration = self.systemConfig,
                                        applicationName = self.applicationName,
                                        applicationVersion = self.applicationVersion,
                                        extraPackages = self.extraPackages,
                                        runTimeProject = self.runTimeProjectName,
                                        runTimeProjectVersion = self.runTimeProjectVersion,
                                        poolXMLCatalogName = self.poolXMLCatName )

        if not result['OK']:
          self.log.error( "Could not obtain project environment with result: %s" % ( result ) )
          return result  # this will distinguish between LbLogin / SetupProject / actual application failures
        projectEnvironment = result['Value']

      # Creating the command
      if self.executable == 'gaudirun.py':
        gaudiRunFlags = self.opsH.getValue( '/GaudiExecution/gaudirunFlags', 'gaudirun.py' )
      else:
        gaudiRunFlags = self.executable
      if self.multicoreJob == 'True':
        if self.multicoreStep.upper() == 'Y':
          cpus = multiprocessing.cpu_count()
          if cpus > 1:
            if multicoreWN():
              gaudiRunFlags = gaudiRunFlags + ' --ncpus -1 '
            else:
              self.log.info( "Would have run with option '--ncpus -1', but it is not allowed here" )

      if self.optionsLine or self.jobType.lower() == 'user':
        command = '%s %s %s' % ( gaudiRunFlags, self.optfile, 'gaudi_extra_options.py' )
      else:  # everything but user jobs
        if self.extraOptionsLine:
          fopen = open( 'gaudi_extra_options.py', 'w' )
          fopen.write( self.extraOptionsLine )
          fopen.close()
          command = '%s %s %s %s' % ( gaudiRunFlags, self.optfile, prodConfFileName, 'gaudi_extra_options.py' )
        else:
          command = '%s %s %s' % ( gaudiRunFlags, self.optfile, prodConfFileName )
      print 'Command = %s' % ( command )  # Really print here as this is useful to see

      # Set some parameter names
      dumpEnvName = 'Environment_Dump_%s_%s_Step%s.log' % ( self.applicationName,
                                                            self.applicationVersion,
                                                            self.step_number )
      scriptName = '%s_%s_Run_%s.sh' % ( self.applicationName,
                                         self.applicationVersion,
                                         self.step_number )
      coreDumpName = '%s_Step%s' % ( self.applicationName,
                                     self.step_number )

      # Wrap final execution command with defaults
      finalCommand = addCommandDefaults( command,
                                         envDump = dumpEnvName,
                                         coreDumpLog = coreDumpName )['Value']  # should always be S_OK()

      # Create debug shell script to reproduce the application execution
      debugResult = createDebugScript( scriptName,
                                       command,
                                       env = projectEnvironment,
                                       envLogFile = dumpEnvName,
                                       coreDumpLog = coreDumpName )  # will add command defaults internally
      if debugResult['OK']:
        self.log.verbose( 'Created debug script %s for Step %s' % ( debugResult['Value'], self.step_number ) )

      if os.path.exists( self.applicationLog ):
        os.remove( self.applicationLog )

      self.log.info( 'Running %s %s step %s' % ( self.applicationName, self.applicationVersion, self.step_number ) )
      self.setApplicationStatus( '%s %s step %s' % ( self.applicationName, self.applicationVersion, self.step_number ) )
      result = shellCall( 0, finalCommand,
                          env = projectEnvironment,
                          callbackFunction = self.redirectLogOutput,
                          bufferLimit = 20971520 )
      if not result['OK']:
        raise RuntimeError( "Problem Executing Application" )

      resultTuple = result['Value']
      status = resultTuple[0]

      self.log.info( "Status after the application execution is %s" % str( status ) )
      if status != 0:
        if self.jobType.lower() == 'sam':
          self.workflow_commons.setdefault( 'SAMResults', {} )[self.applicationName] = 'CRITICAL'
          self.workflow_commons.setdefault( 'SAMDetails', {} )[self.applicationName] = "Error: %s %s %s." % \
                                                      ( self.applicationName, self.applicationVersion, self.stdError )
        self.log.error( "%s execution completed with errors" % self.applicationName )
        self.log.error( "==================================\n StdError:\n" )
        self.log.error( self.stdError )
        self.log.error( '%s Exited With Status %s' % ( self.applicationName, status ) )
        raise RuntimeError( "%s Exited With Status %s" % ( self.applicationName, status ) )
      else:
        self.log.info( "%s execution completed successfully" % self.applicationName )

      self.log.info( "Going to manage %s output" % self.applicationName )
      self._manageAppOutput( stepOutputs )

      # Still have to set the application status e.g. user job case.
      self.setApplicationStatus( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

      if self.jobType.lower() == 'sam':
        # extend workflow_commons for publishing to nagios
        # try to catch the error of accessing second index if first is empty
        self.workflow_commons.setdefault( 'SAMResults', {} )[self.applicationName] = 'OK'
        self.workflow_commons.setdefault( 'SAMDetails', {} )[self.applicationName] = \
                             "%s %s Successful" % ( self.applicationName, self.applicationVersion )

      return S_OK( "%s %s Successful" % ( self.applicationName, self.applicationVersion ) )

    except Exception, e:
      exceptionString = "Exception: %s %s %s." % ( self.applicationName, self.applicationVersion, e )
      if self.jobType.lower() == 'sam':
        self.workflow_commons.setdefault( 'SAMResults', {} )[self.applicationName] = 'CRITICAL'
        self.workflow_commons.setdefault( 'SAMDetails', {} )[self.applicationName] = exceptionString
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( GaudiApplication, self ).finalize( self.version )

  #############################################################################

  def redirectLogOutput( self, fd, message ):
    """ Callback function for the Subprocess.shellcall
        Manages log files

        fd is stdin/stderr
        message is every line (?)
    """
    sys.stdout.flush()
    if message:
      if re.search( 'INFO Evt', message ):
        print message
      if re.search( 'Reading Event record', message ):
        print message
      if self.applicationLog:
        log = open( self.applicationLog, 'a' )
        log.write( message + '\n' )
        log.close()
      else:
        self.log.error( "Application Log file not defined" )
      if fd == 1:
        self.stdError += message

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
