########################################################################
# $Id$
########################################################################

""" Analyse log file(s) module
"""

__RCSID__ = "$Id$"

import os

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Resources.Catalog.PoolXMLFile import getGUID

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary, analyseXMLSummary

class AnalyseXMLSummary( ModuleBase ):
  """ Analysing the XML summary
  """

  def __init__( self ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( 'AnalyseXMLSummary' )
    super( AnalyseXMLSummary, self ).__init__( self.log )

    self.version = __RCSID__
    self.site = DIRAC.siteName()
    self.applicationName = ''
    self.applicationVersion = ''

    #Resolved to be the input data of the current step
    self.stepInputData = []
    #Dict of input data for the job and status
    self.jobInputData = {}

################################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None,
               nc = None, rm = None, logAnalyser = None,
               bk = None, xf_o = None ):
    """ Main execution method. 
    """

    try:
      super( AnalyseXMLSummary, self ).execute( self.version,
                                                production_id, prod_job_id, wms_job_id,
                                                workflowStatus, stepStatus,
                                                wf_commons, step_commons,
                                                step_number, step_id )

      self._resolveInputVariables()

      if not rm:
        from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
        rm = ReplicaManager()

      if self.workflow_commons.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
        self.log.info( 'AnalyseLogFile has already run for this workflow and finalized with sending an error email' )
        return S_OK()

      self.log.info( "Performing XML summary analysis for %s" % ( self.XMLSummary ) )
      # Resolve the step and job input data

      if not xf_o:
        self.XMLSummary_o = XMLSummary( self.XMLSummary, log = self.log )
      else:
        self.XMLSummary_o = xf_o
      self.step_commons['XMLSummary_o'] = self.XMLSummary_o

      if not logAnalyser:
        analyseXMLSummaryResult = analyseXMLSummary( xf_o = self.XMLSummary_o, log = self.log )
      else:
        analyseXMLSummaryResult = logAnalyser( xf_o = self.XMLSummary_o, log = self.log )

      if not analyseXMLSummaryResult['OK']:
        self.log.error( analyseXMLSummaryResult['Message'] )
        self._finalizeWithErrors( analyseXMLSummaryResult['Message'], nc, rm, bk )

        self._updateFileStatus( self.jobInputData, "Unused", int( self.production_id ), rm, self.fileReport )
        # return S_OK if the Step already failed to avoid overwriting the error
        if not self.stepStatus['OK']:
          return S_OK()
        self.setApplicationStatus( analyseXMLSummaryResult['Message'] )
        return analyseXMLSummaryResult

      # if the log looks ok but the step already failed, preserve the previous error
      elif not self.stepStatus['OK']:
        self._updateFileStatus( self.jobInputData, "Unused", int( self.production_id ), rm, self.fileReport )
        return S_OK()

      else:
#        self.log.info( 'Setting numberOfEventsInput to %s' % self.XMLSummary_o.inputEventsTotal )
#        self.log.info( 'Setting numberOfEventsOutput to %s' % self.XMLSummary_o.outputEventsTotal )

        # If the job was successful Update the status of the files to processed
        self.log.info( 'XML summary %s, %s' % ( self.XMLSummary, analyseXMLSummaryResult['Value'] ) )
        self.setApplicationStatus( '%s Step OK' % self.applicationName )

#        self.step_commons['numberOfEventsInput'] = self.XMLSummary_o.inputEventsTotal
#        self.step_commons['numberOfEventsOutput'] = self.XMLSummary_o.outputEventsTotal
        self._updateFileStatus( self.jobInputData, "Processed", int( self.production_id ), rm, self.fileReport )

        return S_OK()

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( AnalyseXMLSummary, self ).finalize( self.version )


################################################################################
# AUXILIAR FUNCTIONS
################################################################################

  def _resolveInputVariables( self ):
    """ By convention any workflow parameters are resolved here.
    """

    super( AnalyseXMLSummary, self )._resolveInputVariables()

    self.applicationName = self.step_commons['applicationName']
    self.applicationVersion = self.step_commons['applicationVersion']

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

    self.XMLSummary = self.step_commons['XMLSummary']

################################################################################

  def _updateFileStatus( self, inputs, defaultStatus, prod_id, rm, fr ):
    """ Allows to update file status to a given default, important statuses are
        not overwritten.
    """
    for fileName in inputs.keys():
      stat = inputs[fileName]
      if stat == 'Unused':
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
        result = rm.putAndRegister( lfn, fname, 'CERN-DEBUG',
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

    self.workflow_commons['AnalyseLogFilePreviouslyFinalized'] = True

################################################################################
# END AUXILIAR FUNCTIONS
################################################################################

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
