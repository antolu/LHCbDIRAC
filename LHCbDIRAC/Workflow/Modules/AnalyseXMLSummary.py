""" Analyse XMLSummary module
"""

__RCSID__ = "$Id$"

import os

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Resources.Catalog.PoolXMLFile import getGUID
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary


class AnalyseXMLSummary( ModuleBase ):
  """ Analysing the XML summary
  """

  def __init__( self, bkClient = None, rm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( 'AnalyseXMLSummary' )
    super( AnalyseXMLSummary, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )

    self.version = __RCSID__
    self.site = DIRAC.siteName()
    self.nc = NotificationClient()
    self.XMLSummary = ''

  def _resolveInputVariables( self ):
    """ By convention any workflow parameters are resolved here.
    """

    super( AnalyseXMLSummary, self )._resolveInputVariables()
    super( AnalyseXMLSummary, self )._resolveInputStep()

    self.XMLSummary_o = XMLSummary( self.XMLSummary, log = self.log )

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main execution method.

        Here we analyse what is written in the XML summary, and take decisions accordingly
    """

    try:
      super( AnalyseXMLSummary, self ).execute( self.version,
                                                production_id, prod_job_id, wms_job_id,
                                                workflowStatus, stepStatus,
                                                wf_commons, step_commons,
                                                step_number, step_id )

      self._resolveInputVariables()

      self.log.info( "Performing XML summary analysis for %s" % ( self.XMLSummary ) )
      # Resolve the step and job input data

      self.step_commons['XMLSummary_o'] = self.XMLSummary_o
      if self.XMLSummary_o.success == 'True' \
        and self.XMLSummary_o.step == 'finalize' \
        and self.XMLSummary_o._outputsOK() \
        and not self.XMLSummary_o.inputFileStats['mult'] \
        and not self.XMLSummary_o.inputFileStats['other']:
        # basic success, now check for failures in the input files
        self._basicSuccess()
      else:
        # here fails!
        if self.workflow_commons.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
          self.log.info( 'AnalyseLogFile has already run for this workflow and finalized with sending an error email' )
          return S_OK()

        self._finalizeWithErrors( "XMLSummary reports error" )

        # return S_OK if the Step already failed to avoid overwriting the error
        if not self.stepStatus['OK']:
          return S_OK()

        self.setApplicationStatus( "XMLSummary reports error" )
        return S_ERROR( "XMLSummary reports error" )

      # if the log looks ok but the step already failed, preserve the previous error
      if not self.stepStatus['OK']:
        return S_OK()

      else:
        self.log.info( 'XML summary %s' % self.XMLSummary )
        self.setApplicationStatus( '%s Step OK' % self.applicationName )

        return S_OK()

    except Exception, e:
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( AnalyseXMLSummary, self ).finalize( self.version )


################################################################################
# AUXILIAR FUNCTIONS
################################################################################

  def _basicSuccess( self ):
    """ Treat basic success
    """
    if self.XMLSummary_o.inputFileStats['part']:
      if self.numberOfEvents != '-1':
        # this is not an error
        pass
      else:
        # report to FileReport
        filesInPart = [x[0].strip( 'LFN:' ) for x in self.XMLSummary_o.inputStatus if x[1] == 'part']
        self.log.error( "Files %s are in status 'part'" % ';'.join( filesInPart ) )
        for fileInPart in filesInPart:
          if fileInPart in self.inputDataList:
            self.log.error( "Reporting %s as 'Problematic'" % fileInPart )
            self.fileReport.setFileStatus( int( self.production_id ), fileInPart, 'Problematic' )
    if self.XMLSummary_o.inputFileStats['fail']:
      # report to FileReport
      filesInFail = [x[0].strip( 'LFN:' ) for x in self.XMLSummary_o.inputStatus if x[1] == 'fail']
      self.log.error( "Files %s are in status 'fail'" % ';'.join( filesInFail ) )
      for fileInFail in filesInFail:
        if fileInFail in self.inputDataList:
          self.log.error( "Reporting %s as 'Problematic'" % fileInFail )
          self.fileReport.setFileStatus( int( self.production_id ), fileInFail, 'Problematic' )

  def _finalizeWithErrors( self, subj ):
    """ Method that sends an email and uploads intermediate job outputs.
    """
    self.log.error( subj )
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
      self.log.error( 'Could not create production LFNs with message "%s"' % ( result['Message'] ) )
      raise Exception( result['Message'] )

    if not result['Value'].has_key( 'DebugLFNs' ):
      self.log.error( 'No debug LFNs found after creating production LFNs, result was:%s' % result )
      raise Exception( 'DebugLFNs Not Found' )

    debugLFNs = result['Value']['DebugLFNs']

    subject = '[' + self.site + '][' + self.applicationName + '] ' + self.applicationVersion + \
              ": " + subj + ' ' + self.production_id + '_' + self.prod_job_id + ' JobID=' + str( self.jobID )
    msg = 'The Application ' + self.applicationName + ' ' + self.applicationVersion + ' had a problem \n'
    msg = msg + 'at site ' + self.site + '\n'
    msg = msg + 'JobID is ' + str( self.jobID ) + '\n'
    msg = msg + 'JobName is ' + self.production_id + '_' + self.prod_job_id + '\n'

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
        result = self.rm.putAndRegister( lfn, fname, 'CERN-DEBUG',
                                         guidInput, catalog = 'LcgFileCatalogCombined' )
        self.log.info( result )
        if not result['OK']:
          self.log.error( 'Could not save INPUT data file with result', str( result['Message'] ) )
          msg += 'Could not save intermediate data file %s with result\n%s\n' % ( fname, result['Message'] )
        else:
          msg = msg + lfn + '\n' + str( result ) + '\n'
      else:
        self.log.info( 'JOBID is null, would have attempted to upload: LFN:%s, file %s, GUID %s to CERN-DEBUG'
                       % ( lfn, fname, guidInput ) )

    if not self._WMSJob():
      self.log.info( "JOBID is null, *NOT* sending mail, for information the mail was:\n====>Start\n%s\n<====End"
                     % ( msg ) )
    else:
      mailAddress = self.opsH.getValue( 'EMail/JobFailures', 'lhcb-datacrash@cern.ch' )
      self.log.info( 'Sending crash mail for job to %s' % ( mailAddress ) )

      res = self.nc.sendMail( mailAddress, subject, msg, 'joel.closier@cern.ch', localAttempt = False )
      if not res['OK']:
        self.log.warn( "The mail could not be sent" )

    self.workflow_commons['AnalyseLogFilePreviouslyFinalized'] = True

################################################################################
# END AUXILIAR FUNCTIONS
################################################################################

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
