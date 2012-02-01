########################################################################
# $Id$
########################################################################

""" Analyse log file(s) module
"""

__RCSID__ = "$Id$"

import os, re, glob

from DIRAC.Resources.Catalog.PoolXMLFile                 import getGUID

from LHCbDIRAC.Core.Utilities.ProductionData             import getLogPath, constructProductionLFNs
from LHCbDIRAC.Workflow.Modules.ModuleBase               import ModuleBase

from DIRAC import S_OK, S_ERROR, gLogger
import DIRAC

class AnalyseLogFile( ModuleBase ):
  """ Analyse not only the XML summary, also the log file is inspected
  """

  def __init__( self ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( 'AnalyseXMLSummary' )
    super( AnalyseLogFile, self ).__init__( self.log )

    self.version = __RCSID__
    self.site = DIRAC.siteName()
    self.systemConfig = ''
    self.numberOfEventsInput = ''
    self.numberOfEventsOutput = ''
    self.numberOfEvents = ''
    self.applicationName = ''
    self.applicationLog = ''
    self.applicationVersion = ''
    self.logFilePath = ''
    self.coreFile = ''
    self.logString = ''
    self.jobType = ''
#    self.stepName             = ''

    #Resolved to be the input data of the current step
    self.stepInputData = []
    #Dict of input data for the job and status
    self.jobInputData = {}

################################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None,
               nc = None, rm = None, logAnalyser = None, bk = None ):
    """ Main execution method. 
    """

    try:
      super( AnalyseLogFile, self ).execute( self.version,
                                                production_id, prod_job_id, wms_job_id,
                                                workflowStatus, stepStatus,
                                                wf_commons, step_commons,
                                                step_number, step_id )

      self._resolveInputVariables( bk )

      if not rm:
        from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
        rm = ReplicaManager()

      if self.workflow_commons.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
        self.log.info( 'AnalyseLogFile has already run for this workflow and finalized with sending an error email' )
        return S_OK()

      self.log.verbose( "Performing log file analysis for %s" % ( self.applicationLog ) )
      # Resolve the step and job input data
      self.log.info( 'Resolved the step input data to be:\n%s' % '\n'.join( self.stepInputData ) )
      self.log.info( 'Resolved the job input data to be:\n%s' % '\n'.join( self.jobInputData.keys() ) )

      #First check for the presence of any core dump files caused by an abort of some kind
      for fileInDir in os.listdir( '.' ):
        if re.search( 'coredump', fileInDir.lower() ):
          self.coreFile = fileInDir
          self.log.error( 'Found a core dump file in the current working directory: %s' % self.coreFile )
          self._finalizeWithErrors( 'Found a core dump file in the current working directory: %s'
                                    % self.coreFile, nc, rm, bk )
          self._updateFileStatus( self.jobInputData, 'ApplicationCrash',
                                  int( self.production_id ), rm, self.fileReport )
          # return S_OK if the Step already failed to avoid overwriting the error
          if not self.stepStatus['OK']:
            return S_OK()

          return S_ERROR( '%s %s Core Dump' % ( self.applicationName, self.applicationVersion ) )

      if not logAnalyser:
        from LHCbDIRAC.Core.Utilities.ProductionLogs import analyseLogFile
        analyseLogResult = analyseLogFile( fileName = self.applicationLog,
                                           applicationName = self.applicationName,
                                           prod = self.production_id,
                                           job = self.prod_job_id,
                                           stepName = self.step_id,
                                           jobType = self.jobType,
                                           log = self.log )
      else:
        analyseLogResult = logAnalyser( fileName = self.applicationLog,
                                        applicationName = self.applicationName,
                                        prod = self.production_id,
                                        job = self.prod_job_id,
                                        stepName = self.step_id,
                                        jobType = self.jobType,
                                        log = self.log )

      if not analyseLogResult['OK']:
        self.log.error( analyseLogResult )
        if analyseLogResult.has_key( 'Data' ):
          fNameStatusDict = analyseLogResult['Data']
          fNameLFNs = {}
          for lfn in self.jobInputData.keys():
            for fName, status in fNameStatusDict.items():
              if os.path.basename( lfn ) == fName:
                fNameLFNs[lfn] = status
          for lfn, status in fNameLFNs.items():
            self.jobInputData[lfn] = status

        self._finalizeWithErrors( analyseLogResult['Message'], nc, rm, bk )

        self._updateFileStatus( self.jobInputData, "Unused", int( self.production_id ), rm, self.fileReport )
        # return S_OK if the Step already failed to avoid overwriting the error
        if not self.stepStatus['OK']:
          return S_OK()
        self.setApplicationStatus( analyseLogResult['Message'] )
        return analyseLogResult

      # if the log looks ok but the step already failed, preserve the previous error
      elif not self.stepStatus['OK']:
        self._updateFileStatus( self.jobInputData, "Unused", int( self.production_id ), rm, self.fileReport )
        return S_OK()

      else:
        if analyseLogResult.has_key( 'numberOfEventsInput' ):
          self.numberOfEventsInput = analyseLogResult['numberOfEventsInput']
          self.log.info( 'Setting numberOfEventsInput to %s' % self.numberOfEventsInput )

        if analyseLogResult.has_key( 'numberOfEventsOutput' ):
          self.numberOfEventsOutput = analyseLogResult['numberOfEventsOutput']
          self.log.info( 'Setting numberOfEventsOutput to %s' % self.numberOfEventsOutput )

        # If the job was successful Update the status of the files to processed
        self.log.info( 'Log file %s, %s' % ( self.applicationLog, analyseLogResult['Value'] ) )
        self.setApplicationStatus( '%s Step OK' % self.applicationName )

        self.step_commons['numberOfEventsInput'] = self.numberOfEventsInput
        self.step_commons['numberOfEventsOutput'] = self.numberOfEventsOutput
        self._updateFileStatus( self.jobInputData, "Processed", int( self.production_id ), rm, self.fileReport )

        return S_OK()

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( AnalyseLogFile, self ).finalize( self.version )


################################################################################
# AUXILIAR FUNCTIONS
################################################################################

  def _resolveInputVariables( self, bk = None ):
    """ By convention any workflow parameters are resolved here.
    """

    super( AnalyseLogFile, self )._resolveInputVariables()

    if self.workflow_commons.has_key( 'SystemConfig' ):
      self.systemConfig = self.workflow_commons['SystemConfig']

    if not self.step_commons.has_key( 'applicationName' ):
      raise ValueError, 'Step does not have an applicationName'

    self.applicationName = self.step_commons['applicationName']

    if self.workflow_commons.has_key( 'InputData' ):
      if self.workflow_commons['InputData']:
        self.jobInputData = self.workflow_commons['InputData']

    if self.step_commons.has_key( 'inputData' ):
      if self.step_commons['inputData']:
        self.stepInputData = self.step_commons['inputData']

    if self.stepInputData:
      self.log.info( 'Input data defined in workflow for this Gaudi Application step' )
      if type( self.stepInputData ) != type( [] ):
        self.stepInputData = self.stepInputData.split( ';' )

    if self.jobInputData:
      self.log.info( 'All input data for workflow taken from JDL parameter' )
      if type( self.jobInputData ) != type( [] ):
        self.jobInputData = self.jobInputData.split( ';' )
      jobStatusDict = {}
      #clumsy but now make this a dictionary with default "OK" status for all input data
      for lfn in self.jobInputData:
        jobStatusDict[lfn.replace( 'LFN:', '' )] = 'OK'
      self.jobInputData = jobStatusDict
    else:
      self.log.verbose( 'Job has no input data requirement' )

    if self.step_commons.has_key( 'applicationVersion' ):
      self.applicationVersion = self.step_commons['applicationVersion']

    if self.step_commons.has_key( 'applicationLog' ):
      self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key( 'numberOfEvents' ):
      self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key( 'numberOfEventsInput' ):
      self.numberOfEventsInput = self.step_commons['numberOfEventsInput']

    if self.step_commons.has_key( 'numberOfEventsOutput' ):
      self.numberOfEventsOutput = self.step_commons['numberOfEventsOutput']

    #Use LHCb utility for local running via jobexec
    if self.workflow_commons.has_key( 'LogFilePath' ):
      self.logFilePath = self.workflow_commons['LogFilePath']
      if type( self.logFilePath ) == type( [] ):
        self.logFilePath = self.logFilePath[0]
    else:
      self.log.info( 'LogFilePath parameter not found, creating on the fly' )
      result = getLogPath( self.workflow_commons, bk )
      if not result['OK']:
        self.log.error( 'Could not create LogFilePath', result['Message'] )
        raise Exception, result['Message']
      self.logFilePath = result['Value']['LogFilePath'][0]

#    if self.step_commons.has_key( 'name' ):
#      self.stepName = self.step_commons['name']

    if self.workflow_commons.has_key( 'JobType' ):
      self.jobType = self.workflow_commons['JobType']

################################################################################

  def _updateFileStatus( self, inputs, defaultStatus, prod_id, rm, fr ):
    """ Allows to update file status to a given default, important statuses are
        not overwritten.
    """
    for fileName in inputs.keys():
      stat = inputs[fileName]
      #FIXME: this part is actually never EVER reached
#      if stat == "Problematic":
#        res = self.setReplicaProblematic( lfn = fileName, se = self.site, reason = 'Problematic', rm = rm )
      if stat in ['Unused', 'ApplicationCrash']:
        self.log.info( "%s will be updated to status '%s'" % ( fileName, stat ) )
      else:
        stat = defaultStatus
        self.log.info( "%s will be updated to default status '%s'" % ( fileName, defaultStatus ) )
      self.setFileStatus( prod_id, lfn = fileName, status = stat, fileReport = fr )


################################################################################

  def _finalizeWithErrors( self, subj, nc, rm, bk = None ):
    """ Method that sends an email and uploads intermediate job outputs.
    """

    if not rm:
      from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
      rm = ReplicaManager()

    self.workflow_commons['AnalyseLogFilePreviouslyFinalized'] = True
    #Have to check that the output list is defined in the workflow commons, this is
    #done by the first BK report module that executes at the end of a step but in 
    #this case the current step 'listoutput' must be added.
    if self.workflow_commons.has_key( 'outputList' ):
      for outputItem in self.step_commons['listoutput']:
        if outputItem not in self.workflow_commons['outputList']:
          self.workflow_commons['outputList'].append( outputItem )
    else:
      self.workflow_commons['outputList'] = self.step_commons['listoutput']

    result = constructProductionLFNs( self.workflow_commons, bk )

    if not result['OK']:
      self.log.error( 'Could not create production LFNs with message "%s"' % ( result['Message'] ) )
      raise Exception, result['Message']

    if not result['Value'].has_key( 'DebugLFNs' ):
      self.log.error( 'No debug LFNs found after creating production LFNs, result was:%s' % result )
      raise Exception, 'DebugLFNs Not Found'

    debugLFNs = result['Value']['DebugLFNs']

    subject = '[' + self.site + '][' + self.applicationName + '] ' + self.applicationVersion + \
              ": " + subj + ' ' + self.production_id + '_' + self.prod_job_id + ' JobID=' + str( self.jobID )
    msg = 'The Application ' + self.applicationName + ' ' + self.applicationVersion + ' had a problem \n'
    msg = msg + 'at site ' + self.site + ' for platform ' + self.systemConfig + '\n'
    msg = msg + 'JobID is ' + str( self.jobID ) + '\n'
    msg = msg + 'JobName is ' + self.production_id + '_' + self.prod_job_id + '\n'

    if self.coreFile:
      self.log.info( 'Will attempt to upload core dump file: %s' % self.coreFile )
      msg += '\n\nCore file found:\n'
      coreLFN = ''
      for lfn in debugLFNs:
        if re.search( '[0-9]+_core', os.path.basename( lfn ) ):
          coreLFN = lfn
      if self._WMSJob():
        if coreLFN:
          self.log.info( 'Attempting: rm.putAndRegister("%s","%s","CERN-DEBUG","catalog="LcgFileCatalogCombined"'
                         % ( coreLFN, self.coreFile ) )
          result = rm.putAndRegister( coreLFN, self.coreFile, 'CERN-DEBUG',
                                      catalog = 'LcgFileCatalogCombined' )
          self.log.info( result )
          if not result['OK']:
            self.log.error( 'Could not save core dump file', result['Message'] )
            msg += 'Could not save dump file with message "%s"\n' % result['Message']
          else:
            msg += coreLFN + '\n'
      else:
        self.log.info( 'JOBID is null, would have attempted to upload: LFN:%s, file %s to CERN-DEBUG' % ( coreLFN, self.coreFile ) )

    toUpload = {}
    for lfn in debugLFNs:
      if os.path.exists( os.path.basename( lfn ) ):
        toUpload[os.path.basename( lfn )] = lfn

    if toUpload:
      msg += '\n\nIntermediate job data files:\n'

    for fname, lfn in toUpload.items():
      guidResult = getGUID( fname )

      guidInput = ''
      if not guidResult['OK']:
        self.log.error( 'Could not find GUID for %s with message' % ( fname ), guidResult['Message'] )
      elif guidResult['generated']:
        self.log.info( 'PoolXMLFile generated GUID(s) for the following files ', ', '.join( guidResult['generated'] ) )
        guidInput = guidResult['Value'][fname]
      else:
        guidInput = guidResult['Value'][fname]

      if self._WMSJob():
        self.log.info( 'Attempting: rm.putAndRegister("%s","%s","CERN-DEBUG","%s","catalog="LcgFileCatalogCombined"'
                       % ( fname, lfn, guidInput ) )
        result = rm.putAndRegister( lfn, fname, 'CERN-DEBUG',
                                    guidInput, catalog = 'LcgFileCatalogCombined' )
        self.log.info( result )
        if not result['OK']:
          self.log.error( 'Could not save INPUT data file with result', str( result ) )
          msg += 'Could not save intermediate data file %s with result\n%s\n' % ( fname, result )
        else:
          msg = msg + lfn + '\n' + str( result ) + '\n'
      else:
        self.log.info( 'JOBID is null, would have attempted to upload: LFN:%s, file %s, GUID %s to CERN-DEBUG'
                       % ( lfn, fname, guidInput ) )

    if self.applicationLog:
      logurl = 'http://lhcb-logs.cern.ch/storage' + self.logFilePath
      msg = msg + '\n\nLog Files directory for the job:\n'
      msg = msg + logurl + '/\n'
      msg = msg + '\n\nLog File for the problematic step:\n'
      msg = msg + logurl + '/' + self.applicationLog + '\n'
      msg = msg + '\n\nJob StdOut:\n'
      msg = msg + logurl + '/std.out\n'
      msg = msg + '\n\nJob StdErr:\n'
      msg = msg + logurl + '/std.err\n'

    globList = glob.glob( '*coredump.log' )
    for check in globList:
      if os.path.isfile( check ):
        self.log.verbose( 'Found locally existing core dump file: %s' % ( check ) )
        fd = open( check )
        contents = fd.read()
        msg = msg + '\n\nCore dump:\n\n' + contents
        fd.close()

    if not self._WMSJob():
      self.log.info( "JOBID is null, *NOT* sending mail, for information the mail was:\n====>Start\n%s\n<====End"
                     % ( msg ) )
    else:
      from DIRAC import gConfig
      mailAddress = gConfig.getValue( '/Operations/EMail/JobFailuresPerSetup/%s'
                                      % gConfig.getValue( "DIRAC/Setup" ) )
      if not mailAddress:
        mailAddress = 'lhcb-datacrash@cern.ch'
      self.log.info( 'Sending crash mail for job to %s' % ( mailAddress ) )

      if not nc:
        from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
        nc = NotificationClient()
      res = nc.sendMail( mailAddress, subject, msg, 'joel.closier@cern.ch', localAttempt = False )
      if not res['OK']:
        self.log.warn( "The mail could not be sent" )

################################################################################
# END AUXILIAR FUNCTIONS
################################################################################

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
