########################################################################
# $Id$
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                        import shellCall
from DIRAC.Resources.Catalog.PoolXMLCatalog                 import PoolXMLCatalog

from LHCbDIRAC.Core.Utilities.ProductionData                import constructProductionLFNs, _makeProductionLFN, _getLFNRoot
from LHCbDIRAC.Core.Utilities.ProductionOptions             import getDataOptions, getModuleOptions
from LHCbDIRAC.Core.Utilities.ProductionEnvironment         import getProjectEnvironment, addCommandDefaults, createDebugScript
from LHCbDIRAC.Core.Utilities.ProductionLogAnalysis         import getDaVinciStreamEvents
from LHCbDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC                                                  import S_OK, S_ERROR, gLogger, gConfig, List
import DIRAC

import re, os, sys, time

class GaudiApplication( ModuleBase ):

  #############################################################################
  def __init__( self ):
    ModuleBase.__init__( self )
    self.enable = True
    self.STEP_NUMBER = ''
    self.version = __RCSID__
    self.debug = True
    self.log = gLogger.getSubLogger( "GaudiApplication" )
    self.jobID = None
    if os.environ.has_key( 'JOBID' ):
      self.jobID = os.environ['JOBID']

    self.optfile = ''
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationName = ''
    self.inputDataType = 'MDF'
    self.numberOfEvents = 0
    self.inputData = '' # to be resolved
    self.InputData = '' # from the (JDL WMS approach)
    self.outputData = ''
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.generator_name = ''
    self.optionsLine = ''
    self.extraPackages = ''
    self.applicationType = ''
    self.jobType = ''
    self.stdError = ''

  #############################################################################
  def resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """
    self.log.verbose( self.workflow_commons )
    self.log.verbose( self.step_commons )

    if self.workflow_commons.has_key( 'SystemConfig' ):
      self.systemConfig = self.workflow_commons['SystemConfig']

    if self.step_commons.has_key( 'applicationName' ):
      self.applicationName = self.step_commons['applicationName']
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key( 'applicationType' ):
      self.applicationType = self.step_commons['applicationType']

    if self.step_commons.has_key( 'numberOfEvents' ):
      self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key( 'optionsFile' ):
      self.optionsFile = self.step_commons['optionsFile']

    if self.step_commons.has_key( 'optionsLine' ):
      self.optionsLine = self.step_commons['optionsLine']

    if self.step_commons.has_key( 'generatorName' ):
      self.generator_name = self.step_commons['generatorName']

    if self.step_commons.has_key( 'extraPackages' ):
      self.extraPackages = self.step_commons['extraPackages']
      if not self.extraPackages == '':
        if type( self.extraPackages ) != type( [] ):
          self.extraPackages = self.extraPackages.split( ';' )

    if self.workflow_commons.has_key( 'poolXMLCatName' ):
      self.poolXMLCatName = self.workflow_commons['poolXMLCatName']

    if self.step_commons.has_key( 'inputDataType' ):
      self.inputDataType = self.step_commons['inputDataType']

    if self.workflow_commons.has_key( 'InputData' ):
      self.InputData = self.workflow_commons['InputData']

    if self.step_commons.has_key( 'inputData' ):
      self.inputData = self.step_commons['inputData']

    #Input data resolution has two cases. Either there is explicitly defined
    #input data for the application step (a subset of total workflow input data reqt)
    #*or* this is defined at the job level and the job wrapper has created a
    #pool_xml_catalog.xml slice for all requested files.

    if self.inputData:
      self.log.info( 'Input data defined in workflow for this Gaudi Application step' )
      if type( self.inputData ) != type( [] ):
        self.inputData = self.inputData.split( ';' )
    elif self.InputData:
      self.log.info( 'Input data defined taken from JDL parameter' )
      if type( self.InputData ) != type( [] ):
        self.inputData = self.InputData.split( ';' )
    else:
      self.log.verbose( 'Job has no input data requirement' )

    if self.workflow_commons.has_key( 'JobType' ):
      self.jobType = self.workflow_commons['JobType']

    #only required until the stripping is the same for MC / data
    if self.workflow_commons.has_key( 'configName' ):
      self.bkConfigName = self.workflow_commons['configName']

    if self.step_commons.has_key( 'listoutput' ):
      self.stepOutputs = self.step_commons['listoutput']

  #############################################################################
  def execute( self ):
    """ The main execution method of GaudiApplication.
    """

    #initialization
    self.resolveInputVariables()
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info( 'Skip this module, failure detected in a previous step :' )
      self.log.info( 'Workflow status : %s' % ( self.workflowStatus ) )
      self.log.info( 'Step Status %s' % ( self.stepStatus ) )
      return S_OK()

    if not self.applicationName or not self.applicationVersion:
      return S_ERROR( 'No Gaudi Application defined' )
    elif not self.systemConfig:
      return S_ERROR( 'No LHCb platform selected' )
    elif not self.applicationLog:
      return S_ERROR( 'No Log file provided' )

    if not self.optionsFile and not self.optionsLine:
      self.log.warn( 'No optionsFile or optionsLine specified in workflow' )

    self.log.info( 'Initializing %s' % ( self.version ) )
    self.root = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
    self.log.info( "Executing application %s %s for system configuration %s" % ( self.applicationName, self.applicationVersion, self.systemConfig ) )
    self.log.verbose( "/LocalSite/Root directory for job is %s" % ( self.root ) )

    ## FIXME: need to agree what the name of the Online Farm is
    if DIRAC.siteName() == 'DIRAC.ONLINE-FARM.ch':
      return self.onlineExecute()

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

    #Prepare standard project run time options
    generatedOpts = 'gaudi_extra_options.py'
    if os.path.exists( generatedOpts ): os.remove( generatedOpts )

    inputDataOpts = getDataOptions( self.applicationName, self.inputData, self.inputDataType, self.poolXMLCatName )['Value'] #always OK
    runNumberGauss = 0
    firstEventNumberGauss = 1
    if self.applicationName.lower() == "gauss" and self.PRODUCTION_ID and self.JOB_ID:
      runNumberGauss = int( self.PRODUCTION_ID ) * 100 + int( self.JOB_ID )
      firstEventNumberGauss = int( self.numberOfEvents ) * ( int( self.JOB_ID ) - 1 ) + 1

    projectOpts = getModuleOptions( self.applicationName, self.numberOfEvents, inputDataOpts, self.optionsLine, runNumberGauss, firstEventNumberGauss, self.jobType )['Value'] #always OK
    self.log.info( 'Extra options generated for %s %s step:' % ( self.applicationName, self.applicationVersion ) )
    print projectOpts #Always useful to see in the logs (don't use gLogger as we often want to cut n' paste)
    options = open( generatedOpts, 'w' )
    options.write( projectOpts )
    options.close()

    #Now obtain the project environment for execution
    result = getProjectEnvironment( self.systemConfig, self.applicationName, self.applicationVersion, self.extraPackages, '', '', '', self.generator_name, self.poolXMLCatName, None )
    if not result['OK']:
      self.log.error( 'Could not obtain project environment with result: %s' % ( result ) )
      return result # this will distinguish between LbLogin / SetupProject / actual application failures

    projectEnvironment = result['Value']
    setup = gConfig.getValue( '/DIRAC/Setup', '' )
    gaudiRunFlags = gConfig.getValue( '/Operations/GaudiExecution/%s/gaudirunFlags' % ( setup ), '' )
    command = '%s %s %s' % ( gaudiRunFlags, self.optfile, generatedOpts )
    print 'Command = %s' % ( command )  #Really print here as this is useful to see

    #Set some parameter names
    dumpEnvName = 'Environment_Dump_%s_%s_Step%s.log' % ( self.applicationName, self.applicationVersion, self.STEP_NUMBER )
    scriptName = '%s_%s_Run_%s.sh' % ( self.applicationName, self.applicationVersion, self.STEP_NUMBER )
    coreDumpName = '%s_Step%s' % ( self.applicationName, self.STEP_NUMBER )

    #Wrap final execution command with defaults
    finalCommand = addCommandDefaults( command, envDump = dumpEnvName, coreDumpLog = coreDumpName )['Value'] #should always be S_OK()

    #Create debug shell script to reproduce the application execution
    debugResult = createDebugScript( scriptName, command, env = projectEnvironment, envLogFile = dumpEnvName, coreDumpLog = coreDumpName ) #will add command defaults internally
    if debugResult['OK']:
      self.log.verbose( 'Created debug script %s for Step %s' % ( debugResult['Value'], self.STEP_NUMBER ) )

    if os.path.exists( self.applicationLog ): os.remove( self.applicationLog )

    self.log.info( 'Running %s %s step %s' % ( self.applicationName, self.applicationVersion, self.STEP_NUMBER ) )
    self.setApplicationStatus( '%s %s step %s' % ( self.applicationName, self.applicationVersion, self.STEP_NUMBER ) )
#    result = {'OK':True,'Value':(0,'Disabled Execution','')}
    result = shellCall( 0, finalCommand, env = projectEnvironment, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520 )
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
#    self._lowerExtension()

    self.log.info( "Going to manage %s output" % self.applicationName )
    self._manageGaudiAppOutput()

    # Still have to set the application status e.g. user job case.
    self.setApplicationStatus( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )
    return S_OK( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

  #############################################################################

  def _manageGaudiAppOutput( self ):

    try:
      finalOutputs, bkFileTypes = self._findOutputs( self.stepOutputs )
    except AttributeError:
      self.log.info( 'Step outputs are not defined (this is normal for SAM jobs)' )
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

    if self.applicationName.lower() == 'davinci' and ( self.applicationType.lower() not in ['dst', 'mdst'] ) :
      #Now will attempt to find the number of output events per stream and convey them via the workflow commons dictionary
      streamEvents = getDaVinciStreamEvents( self.applicationLog, bkFileTypes )
      if streamEvents['OK']:
        streamEvents = streamEvents['Value']
        self.workflow_commons['StreamEvents'] = streamEvents


  #############################################################################

  def _findOutputs( self, stepOutput ):
    """ Find which outputs of those in stepOutput (what are expected to be produced) are effectively produced.
        stepOutput, as called here, corresponds to step_commons['listoutputs']

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
        self.log.warn( '%s not found' % output['outputDataName'] )

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
    sys.stdout.flush()
    if message:
      if re.search( 'INFO Evt', message ): print message
      if re.search( 'Reading Event record', message ): print message
      if self.applicationLog:
        log = open( self.applicationLog, 'a' )
        log.write( message + '\n' )
        log.close()
      else:
        self.log.error( "Application Log file not defined" )
      if fd == 1:
        self.stdError += message

  #############################################################################
  def onlineExecute( self ):
    """Use for the Online Farm."""
    import xmlrpclib
    from operator import itemgetter
    xmlrpcerror = "Cannot connect to RecoManager"
    matcherror = "Cannot find slice in RecoManager"
    inputoutputerror = "Input/Output data error"
    # 0: checks
    if not self.workflow_commons.has_key( 'dataType' ):
      return S_ERROR( inputoutputerror )
    dataType = self.workflow_commons[ 'dataType' ].lower()
    if not self.workflow_commons.has_key( 'configName' ):
      return S_ERROR( inputoutputerror )
    configName = self.workflow_commons['configName']
    if self.workflow_commons.has_key( 'configVersion' ):
      configVersion = self.workflow_commons['configVersion']
    else:
      configVersion = self.applicationVersion
    if not self.step_commons.has_key( 'outputData' ):
      return S_ERROR( inputoutputerror )
    if not self.step_commons.has_key( 'listoutput' ):
      return S_ERROR( inputoutputerror )
    outputDataName = None
    outputDataType = None
    for output in self.step_commons[ 'listoutput' ]:
      if output[ 'outputDataName' ] == self.step_commons[ 'outputData' ]:
        outputDataName = output[ 'outputDataName' ]
        outputDataType = output[ 'outputDataType' ]
        break
    if not ( outputDataType and outputDataName ):
      return S_ERROR( inputoutputerror )
    if not 'applicationLog' in self.step_commons:
      return S_ERROR( "No log file specified" )
    # First: Get the full requirements for the job.
    bkProcessingPass = self.workflow_commons[ 'BKProcessingPass' ]
    step = 'Step%d' % ( int( self.STEP_NUMBER ) - 1 )
    bkProcessingPass[ step ][ 'ExtraPackages' ] = List.fromChar( bkProcessingPass[ step ][ 'ExtraPackages' ] , ';' )
    bkProcessingPass[ step ][ 'OptionFiles' ] = List.fromChar( bkProcessingPass[ step ][ 'OptionFiles' ] , ';' )
    # Second: get the application configuration from the RecoManager XMLRPC
    recoManager = xmlrpclib.ServerProxy( 'http://storeio01.lbdaq.cern.ch:8889' )
    # recoManager = DummyRPC()
    try:
      result = recoManager.sliceStatus()
    except:
      self.log.exception()
      return S_ERROR( xmlrpcerror )
    if not result[ 'OK' ]:
      self.log.error( result[ 'Message' ] )
      return S_ERROR( matcherror )
    # Third: find slices which match the given configuration options
    validSlices = {}
    for sliceName in result[ 'Value' ]:
      sliceConfig = result[ 'Value' ][ sliceName ][ 'config' ]
      self.log.debug( 'Comparing:%s and %s' % ( bkProcessingPass[ step ] , sliceConfig ) )
      if self.compareConfigs( bkProcessingPass[ step ] , sliceConfig ):
        validSlices[ sliceName ] = result[ 'Value' ][ sliceName ][ 'availability' ]
    if len( validSlices ) == 0:
      self.log.error( "No slice found matching configuration" )
      return S_ERROR( matcherror )
    # Fourth: find which of the matching slices is better for job sending (more availability)
    sliceName = sorted( validSlices.iteritems(), key = itemgetter( 1 ), reverse = True )[0][0]
    # Fifth: submit the file and wait.
    inputData = self.inputData.lstrip( 'LFN:' ).lstrip( 'lfn:' )
    lfnRoot = _getLFNRoot( inputData, configName, configVersion )
    outputFile = _makeProductionLFN( self.JOB_ID, lfnRoot, ( outputDataName, outputDataType ), dataType, self.PRODUCTION_ID )
    outputFile = outputFile.lstrip( 'LFN:' ).lstrip( 'lfn:' )
    poolXMLCatalog = PoolXMLCatalog( self.poolXMLCatName )
    try:
      guid = poolXMLCatalog.getGuidByLfn( inputData )
    except:
      self.log.exception()
      return S_ERROR( "Error getting GUID for inputfile" )
    logFile = os.path.abspath( self.step_commons[ 'applicationLog' ] )
    try:
      result = recoManager.submitJob( sliceName, inputData , outputFile , logFile, guid )
    except:
      self.log.exception()
      return S_ERROR( xmlrpcerror )
    if not result[ 'OK' ]:
      # if 'fileID' in result['Value']:
      #   fileID = result[ 'Value' ]
      #   res = recoManager.getJobOutput(fileID)
        # log = res['Value']['log']
        # writeLogFromList( loglines )
      self.log.error( "Error running job" , result[ 'Message' ] )
      return S_ERROR( "Error submiting job" )
    # The submission went well
    if os.path.exists( self.applicationLog ):
      os.remove( self.applicationLog )
    self.setApplicationStatus( '%s %s step %s' % ( self.applicationName, self.applicationVersion, self.STEP_NUMBER ) )
    jobID = result[ 'Value' ]
    retrycount = 0
    while True:
      time.sleep( 20 )
      try:
        ret = recoManager.jobStatus( jobID )
      except:
        self.log.exception()
        return S_ERROR( xmlrpcerror )
      if not ret[ 'OK' ]:
        retrycount = retrycount + 1
        if retrycount > 5:
          return S_ERROR( ret[ 'Message' ] )
        continue
      retrycount = 0
      jobstatus = ret[ 'Value' ]
      if jobstatus in [ 'DONE' , 'ERROR' ]:
        ret = recoManager.getJobOutput( jobID )
        if not ret[ 'OK' ]:
          outputError = "Error retrieving output of jobID: %s" % jobID
          self.log.error( outputError , ret[ 'Message' ] )
          return S_ERROR( outputError )
        jobInfo = ret[ 'Value' ] # ( status , inputevents , outputevents , logfile , path )
        # Hack: create symlink to output data
        for path in jobInfo[ 'path' ].values():
          os.symlink( path , os.path.basename( path ) )
        self.step_commons[ 'numberOfEventsInput' ] = str( jobInfo[ 'eventsRead' ] )
        self.step_commons[ 'numberOfEventsOutput' ] = str( jobInfo[ 'eventsWritten' ] )
        self.step_commons[ 'md5' ] = jobInfo[ 'md5' ]
        self.step_commons[ 'guid' ] = jobInfo[ 'guid' ]
        # loglines = jobInfo[ 'log' ]
        # writeLogFromList( loglines )
        self.log.info( "Status after the application execution is %s" % jobstatus )
        failed = False
        if jobstatus == 'ERROR':
          self.log.error( "%s execution completed with errors:" % self.applicationName )
          failed = True
        else:
          self.log.info( "%s execution completed succesfully:" % self.applicationName )
        if failed:
          self.log.error( '%s Exited With Status %s' % ( self.applicationName, jobstatus ) )
          return S_ERROR( '%s Exited With Status %s' % ( self.applicationName, jobstatus ) )
        self.setApplicationStatus( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )
        print self.step_commons
        return S_OK( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

  #############################################################################

  def compareConfigs( self , config1 , config2 ): # FIXME
    if len( config1.keys() ) != len( config2.keys() ):
      return False
    for key in config1:
      if not key in config2:
        return False
      else:
        if key == 'ExtraPackages':
          if not sorted( config2[ 'ExtraPackages' ] ) == sorted( config1[ 'ExtraPackages' ] ):
            return False
        elif config1[ key ] != config2[ key ]:
          return False
    return True

  # def writeLogFromList( self , loglines ):
  #   log = open( self.step_commons[ 'applicationLog' ] , 'w' )
  #   for line in loglines:
  #     log.write( "%s\n" %line )
  #   log.close()

  #############################################################################

  def _lowerExtension( self ):
    """
      Lowers the file extension of the produced files (on disk!). 
      E.g.: fileName.EXTens.ION -> fileName.extens.ion
      
      Unused at the moment
    """

    filesInDir = [x for x in os.listdir( '.' ) if not os.path.isdir( x )]

    lowers = []

    for file in filesInDir:
      splitted = file.split( '.' )
      if len( splitted ) > 1:
        lowered = ''
        for toBeLowered in splitted[1:]:
          lowered = lowered + '.' + toBeLowered.lower()
          final = splitted[0] + lowered
      else:
        final = splitted[0]
      lowers.append( ( file, final ) )

    for file in lowers:
      os.rename( file[0], file[1] )


#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
