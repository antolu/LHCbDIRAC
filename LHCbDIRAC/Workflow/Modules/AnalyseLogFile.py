""" Analyse log file(s) module
"""

import os
import re
import glob

from DIRAC                                            import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Catalog.PoolXMLFile              import getGUID
from DIRAC.FrameworkSystem.Client.NotificationClient  import NotificationClient
from DIRAC.DataManagementSystem.Client.DataManager    import DataManager

from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Workflow.Modules.ModuleBase   import ModuleBase
from LHCbDIRAC.Core.Utilities.ProductionLogs import analyseLogFile

__RCSID__ = "$Id$"


class AnalyseLogFile( ModuleBase ):
  """ Analyse not only the XML summary, also the log file is inspected
  """

  def __init__( self, bkClient = None, dm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( 'AnalyseLogFile' )
    super( AnalyseLogFile, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__
    self.logFilePath = ''
    self.coreFile = ''
    self.nc = NotificationClient()
    self.logAnalyser = analyseLogFile

  def _resolveInputVariables( self ):
    """ By convention any workflow parameters are resolved here.
    """

    super( AnalyseLogFile, self )._resolveInputVariables()
    super( AnalyseLogFile, self )._resolveInputStep()

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main execution method.
    """

    try:
      super( AnalyseLogFile, self ).execute( self.version,
                                             production_id, prod_job_id, wms_job_id,
                                             workflowStatus, stepStatus,
                                             wf_commons, step_commons,
                                             step_number, step_id )

      self._resolveInputVariables()

      if self.workflow_commons.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
        self.log.info( "AnalyseLogFile has already run for this workflow and finalized with sending an error email" )
        return S_OK()

      self.log.info( "Performing log file analysis for %s" % ( self.applicationLog ) )
      # Resolve the step and job input data

      analyseLogResult = self.logAnalyser( fileName = self.applicationLog,
                                           applicationName = self.applicationName,
                                           log = self.log )
      if not analyseLogResult:
        self._finalizeWithErrors( "Log reports ERROR" )

        # return S_OK if the Step already failed to avoid overwriting the error
        if not self.stepStatus['OK']:
          return S_OK()
        self.setApplicationStatus( "Log reports ERROR" )
        return S_ERROR( "Log reports ERROR" )

      # if the log looks ok but the step already failed, preserve the previous error
      elif not self.stepStatus['OK']:
        return S_OK()

      else:
        # If the job was successful Update the status of the files to processed
        self.log.info( "Log file %s OK" % self.applicationLog )
        self.setApplicationStatus( "%s Step OK" % self.applicationName )

        return S_OK()

    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in AnalyseLogFile execute module", lException = e )
      self.setApplicationStatus( repr(e) )
      return S_ERROR( str(e) )

    finally:
      super( AnalyseLogFile, self ).finalize( self.version )


################################################################################
# AUXILIAR FUNCTIONS
################################################################################

  def _finalizeWithErrors( self, subj ):
    """ Method that sends an email and uploads intermediate job outputs.
    """
    self.workflow_commons['AnalyseLogFilePreviouslyFinalized'] = True
    # Have to check that the output list is defined in the workflow commons, this is
    # done by the first BK report module that executes at the end of a step but in
    # this case the current step 'listoutput' must be added.
    if self.workflow_commons.has_key( 'outputList' ):
      for outputItem in self.step_commons['listoutput']:
        if outputItem not in self.workflow_commons['outputList']:
          self.workflow_commons['outputList'].append( outputItem )
    else:
      self.workflow_commons['outputList'] = self.step_commons['listoutput']

    result = constructProductionLFNs( self.workflow_commons, self.bkClient )

    if not result['OK']:
      self.log.error( "Could not create production LFNs with message '%s'" % ( result['Message'] ) )
      raise Exception( result['Message'] )

    if not result['Value'].has_key( 'DebugLFNs' ):
      self.log.error( "No debug LFNs found after creating production LFNs, result was:%s" % result )
      raise Exception ( "DebugLFNs Not Found" )

    debugLFNs = result['Value']['DebugLFNs']

    subject = '[' + self.siteName + '][' + self.applicationName + '] ' + self.applicationVersion + \
              ": " + subj + ' ' + self.production_id + '_' + self.prod_job_id + ' JobID=' + str( self.jobID )
    msg = 'The Application ' + self.applicationName + ' ' + self.applicationVersion + ' had a problem \n'
    msg = msg + 'at site ' + self.siteName + '\n'
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
          self.log.info( "Attempting putAndRegister('%s','%s','%s') on master catalog" % ( coreLFN, self.coreFile,
                                                                                           self.debugSE ) )
          result = DataManager( masterCatalogOnly = True ).putAndRegister( coreLFN, self.coreFile, self.debugSE )
          self.log.info( result )
          if not result['OK']:
            self.log.error( 'Could not save core dump file', result['Message'] )
            msg += 'Could not save dump file with message "%s"\n' % result['Message']
          else:
            msg += coreLFN + '\n'
      else:
        self.log.info( "JOBID is null, would have attempted to upload: LFN:%s, file %s to %s" % ( coreLFN,
                                                                                                  self.coreFile,
                                                                                                  self.debugSE ) )

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
        self.log.error( "Could not find GUID for %s with message" % ( fname ), guidResult['Message'] )
      elif guidResult['generated']:
        self.log.info( 'PoolXMLFile generated GUID(s) for the following files ', ', '.join( guidResult['generated'] ) )
        guidInput = guidResult['Value'][fname]
      else:
        guidInput = guidResult['Value'][fname]

      if self._WMSJob():
        self.log.info( 'Attempting putAndRegister("%s","%s","CERN-DEBUG","%s") on master catalog"' % ( fname, lfn,
                                                                                                       guidInput ) )
        result = DataManager( masterCatalogOnly = True ).putAndRegister( lfn, fname, self.debugSE, guidInput )
        self.log.info( result )
        if not result['OK']:
          self.log.error( 'Could not save INPUT data file with result', str( result ) )
          msg += 'Could not save intermediate data file %s with result\n%s\n' % ( fname, result )
        else:
          msg = msg + lfn + '\n' + str( result ) + '\n'
      else:
        self.log.info( 'JOBID is null, would have attempted to upload: LFN:%s, file %s, GUID %s to %s'
                       % ( lfn, fname, guidInput, self.debugSE ) )

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
      mailAddress = self.opsH.getValue( 'EMail/JobFailures', 'lhcb-datacrash@cern.ch' )
      self.log.info( 'Sending crash mail for job to %s' % ( mailAddress ) )

      for mA in mailAddress.replace( ' ', '' ).split( ',' ):
        res = self.nc.sendMail( mA, subject, msg, 'joel.closier@cern.ch', localAttempt = False )
      if not res['OK']:
        self.log.warn( "The mail could not be sent" )

################################################################################
# END AUXILIAR FUNCTIONS
################################################################################

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
