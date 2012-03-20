########################################################################
# $Id$
########################################################################
"""Gaudi Application class"""

__RCSID__ = "$Id$"

import re, os, sys, time, copy

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities.Subprocess  import shellCall

from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Core.Utilities.ProdConf import ProdConf
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectEnvironment, addCommandDefaults, createDebugScript
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
#from LHCbDIRAC.Workflow.Modules.ModulesUtilities import lowerExtension


class GaudiApplication( ModuleBase ):

  #############################################################################

  def __init__( self ):

    self.log = gLogger.getSubLogger( "GaudiApplication" )
    super( GaudiApplication, self ).__init__( self.log )

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
    self.numberOfEvents = 0
    self.stepInputData = '' # to be resolved
    self.outputData = ''
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.generator_name = ''
    self.optionsFile = ''
    self.optionsLine = ''
    self.extraPackages = ''
    self.applicationType = ''
    self.stepOutputsType = []
    self.optionsFormat = ''
    self.histoName = ''
    self.jobType = ''
    self.stdError = ''
    self.DDDBTag = ''
    self.CondDBTag = ''
    self.DQTag = ''

  #############################################################################

  def _resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """

    super( GaudiApplication, self )._resolveInputVariables()
    super( GaudiApplication, self )._resolveInputStep()

    #Input data resolution has two cases. Either there is explicitly defined
    #input data for the application step (a subset of total workflow input data reqt)
    #*or* this is defined at the job level and the job wrapper has created a
    #pool_xml_catalog.xml slice for all requested files.
    if self.stepInputData:
      self.log.info( 'Input data defined in workflow for this Gaudi Application step' )
      if type( self.stepInputData ) != type( [] ):
        self.stepInputData = self.stepInputData.split( ';' )
    elif self.InputData:
      self.log.info( 'Input data defined taken from JDL parameter' )
#      if type( self.InputData ) != type( [] ):
#        self.InputData = self.InputData.split( ';' )
    else:
      self.log.verbose( 'Job has no input data requirement' )

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

      self.root = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
      self.log.info( "Executing application %s %s for system configuration %s" % ( self.applicationName,
                                                                                   self.applicationVersion,
                                                                                   self.systemConfig ) )
      self.log.verbose( "/LocalSite/Root directory for job is %s" % ( self.root ) )

      if self.jobType.lower() == 'merge':
        #Disable the watchdog check in case the file uploading takes a long time
        self.log.info( 'Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog for Merge production' )
        fopen = open( 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
        fopen.write( '%s' % time.asctime() )
        fopen.close()

      #Resolve options files
      if self.optionsFile and not self.optionsFile == "None":
        for fileopt in self.optionsFile.split( ';' ):
          if os.path.exists( '%s/%s' % ( os.getcwd(), os.path.basename( fileopt ) ) ):
            self.optfile += ' ' + os.path.basename( fileopt )
          # Otherwise take the one from the application options directory
          elif re.search( '\$', fileopt ):
            self.log.info( 'Found options file containing environment variable: %s' % fileopt )
            self.optfile += '  %s' % ( fileopt )
          else:
            self.log.error( 'Cannot process options: "%s" not found via environment variable or in local directory' % ( fileopt ) )

      self.log.info( 'Final options files: %s' % ( self.optfile ) )

#      #Prepare standard project run time options
#      generatedOpts = 'gaudi_extra_options.py'
#      if os.path.exists( generatedOpts ):
#        os.remove( generatedOpts )

      runNumberGauss = 0
      firstEventNumberGauss = 0
      if self.applicationName.lower() == "gauss" and self.production_id and self.prod_job_id:
        runNumberGauss = int( self.production_id ) * 100 + int( self.prod_job_id )
        firstEventNumberGauss = int( self.numberOfEvents ) * ( int( self.prod_job_id ) - 1 ) + 1

      prodConfFile = 'prodConf_%s_%s_%s_%s.py' % ( self.applicationName,
                                                   self.production_id,
                                                   self.prod_job_id,
                                                   self.step_number )
      p = ProdConf( prodConfFile )

      optionsDict = {}
      optionsDict['Application'] = self.applicationName
      optionsDict['AppVersion'] = self.applicationVersion
      if self.optionsFormat:
        optionsDict['OptionFormat'] = self.optionsFormat
      if self.stepInputData:
        optionsDict['InputFiles'] = ['LFN:' + x.lstrip('LFN:') for x in self.stepInputData]
      optionsDict['OutputFilePrefix'] = self.outputFilePrefix
      stepOutTypes = copy.deepcopy(self.stepOutputsType)
      if 'HIST' in stepOutTypes:
        stepOutTypes.remove( 'HIST' )
      optionsDict['OutputFileTypes'] = stepOutTypes
      optionsDict['XMLSummaryFile'] = self.XMLSummary
      optionsDict['XMLFileCatalog'] = self.poolXMLCatName
      if self.histoName:
        if 'HIST' in self.stepOutputsType:
          optionsDict['HistogramFile'] = self.histoName
      if self.DDDBTag:
        optionsDict['DDDBTag'] = self.DDDBTag
      if self.CondDBTag:
        optionsDict['CondDBTag'] = self.CondDBTag
      if self.DQTag:
        optionsDict['DQTag'] = self.DQTag
      optionsDict['NOfEvents'] = int( self.numberOfEvents )
      if runNumberGauss:
        optionsDict['RunNumber'] = runNumberGauss
      if firstEventNumberGauss:
        optionsDict['FirstEventNumber'] = firstEventNumberGauss

      p.putOptionsIn( optionsDict )

      if not projectEnvironment:
        #Now obtain the project environment for execution
        result = getProjectEnvironment( systemConfiguration = self.systemConfig,
                                        applicationName = self.applicationName,
                                        applicationVersion = self.applicationVersion,
                                        extraPackages = self.extraPackages,
                                        runTimeProject = self.runTimeProjectName,
                                        runTimeProjectVersion = self.runTimeProjectVersion,
                                        generatorName = self.generator_name,
                                        poolXMLCatalogName = self.poolXMLCatName )

        if not result['OK']:
          self.log.error( 'Could not obtain project environment with result: %s' % ( result ) )
          return result # this will distinguish between LbLogin / SetupProject / actual application failures
        projectEnvironment = result['Value']

      setup = gConfig.getValue( '/DIRAC/Setup', '' )
      gaudiRunFlags = gConfig.getValue( '/Operations/GaudiExecution/%s/gaudirunFlags' % ( setup ), 'gaudirun.py' )
#      command = '%s %s %s' % ( gaudiRunFlags, self.optfile, generatedOpts )
      if self.optionsLine:
        fopen = open( 'gaudi_extra_options.py', 'w' )
        fopen.write( self.optionsLine )
        fopen.close()
        command = '%s %s %s' % ( gaudiRunFlags, self.optfile, prodConfFile, 'gaudi_extra_options.py' )
      else:
        command = '%s %s %s' % ( gaudiRunFlags, self.optfile, prodConfFile )
      print 'Command = %s' % ( command )  #Really print here as this is useful to see

      #Set some parameter names
      dumpEnvName = 'Environment_Dump_%s_%s_Step%s.log' % ( self.applicationName,
                                                            self.applicationVersion,
                                                            self.step_number )
      scriptName = '%s_%s_Run_%s.sh' % ( self.applicationName,
                                         self.applicationVersion,
                                         self.step_number )
      coreDumpName = '%s_Step%s' % ( self.applicationName,
                                     self.step_number )

      #Wrap final execution command with defaults
      finalCommand = addCommandDefaults( command,
                                         envDump = dumpEnvName,
                                         coreDumpLog = coreDumpName )['Value'] #should always be S_OK()

      #Create debug shell script to reproduce the application execution
      debugResult = createDebugScript( scriptName,
                                       command,
                                       env = projectEnvironment,
                                       envLogFile = dumpEnvName,
                                       coreDumpLog = coreDumpName ) #will add command defaults internally
      if debugResult['OK']:
        self.log.verbose( 'Created debug script %s for Step %s' % ( debugResult['Value'], self.step_number ) )

      if os.path.exists( self.applicationLog ): os.remove( self.applicationLog )

      self.log.info( 'Running %s %s step %s' % ( self.applicationName, self.applicationVersion, self.step_number ) )
      self.setApplicationStatus( '%s %s step %s' % ( self.applicationName, self.applicationVersion, self.step_number ) )
  #    result = {'OK':True,'Value':(0,'Disabled Execution','')}
      result = shellCall( 0, finalCommand,
                          env = projectEnvironment,
                          callbackFunction = self.redirectLogOutput,
                          bufferLimit = 20971520 )
      if not result['OK']:
        return S_ERROR( 'Problem Executing Application' )

      resultTuple = result['Value']
      status = resultTuple[0]

      self.log.info( "Status after the application execution is %s" % str( status ) )
      if status != 0:
        self.log.error( "%s execution completed with errors" % self.applicationName )
        self.log.error( "==================================\n StdError:\n" )
        self.log.error( self.stdError )
        self.log.error( '%s Exited With Status %s' % ( self.applicationName, status ) )
        return S_ERROR( '%s Exited With Status %s' % ( self.applicationName, status ) )
      else:
        self.log.info( "%s execution completed succesfully" % self.applicationName )

      #I want to have only files with lower case extension
      #decided NOT to use it for the moment
  #    lowerExtension()

      self.log.info( "Going to manage %s output" % self.applicationName )
      try:
        self._manageGaudiAppOutput()
      except IOError, e:
        return S_ERROR( e )

      # Still have to set the application status e.g. user job case.
      self.setApplicationStatus( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

      return S_OK( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( GaudiApplication, self ).finalize( self.version )

  #############################################################################

  def _manageGaudiAppOutput( self ):

    try:
      finalOutputs, bkFileTypes = self._findOutputs( self.stepOutputs )
    except AttributeError:
      self.log.warn( 'Step outputs are not defined (normal for SAM and user jobs. Not normal in productions)' )
      return

    self.log.info( 'Final step outputs are: %s' % ( finalOutputs ) )
    self.step_commons['listoutput'] = finalOutputs

    if self.workflow_commons.has_key( 'outputList' ):
      for outFile in finalOutputs:
        if outFile not in self.workflow_commons['outputList']:
          self.workflow_commons['outputList'].append( outFile )
    else:
      self.workflow_commons['outputList'] = finalOutputs

    self.log.info( 'Attempting to recreate the production output LFNs...' )
    result = constructProductionLFNs( self.workflow_commons )
    if not result['OK']:
      self.log.error( 'Could not create production LFNs', result['Message'] )
      return result
    self.workflow_commons['BookkeepingLFNs'] = result['Value']['BookkeepingLFNs']
    self.workflow_commons['LogFilePath'] = result['Value']['LogFilePath']
    self.workflow_commons['ProductionOutputData'] = result['Value']['ProductionOutputData']

  #############################################################################

  def _findOutputs( self, stepOutput ):
    """ Find which outputs of those in stepOutput (what are expected to be produced) are effectively produced.
        stepOutput, as called here, corresponds to step_commons['listoutput']

        stepOutput =
        [{'outputDataType': 'BHADRON.DST',
        'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.BHADRON.DST'},
        {'outputDataType': 'CALIBRATION.DST',
        'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.CALIBRATION.DST'},

    """

    bkFileTypes = []
    finalOutputs = []

    filesFound = []

    for output in stepOutput:

      found = False
      for fileOnDisk in os.listdir( '.' ):
        if output['outputDataName'].lower() == fileOnDisk.lower():
          found = True
          break

      if found:
        self.log.info( 'Found output file %s matching %s (case is not considered)' % ( fileOnDisk, output['outputDataName'] ) )
        output['outputDataName'] = fileOnDisk
        filesFound.append( output )
      else:
        self.log.error( '%s not found' % output['outputDataName'] )
        raise IOError, '%s not found' % output['outputDataName']

    for f in filesFound:
      bkFileTypes.append( f['outputDataType'].upper() )
      finalOutputs.append( {'outputDataName': f['outputDataName'],
                            'outputDataType': f['outputDataType'].lower(),
                            'outputDataSE': f['outputDataSE'],
                            'outputBKType': f['outputDataType'].upper()
                            } )

    return ( finalOutputs, bkFileTypes )


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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
