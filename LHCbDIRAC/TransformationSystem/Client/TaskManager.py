""" Extension of DIRAC Task Manager
"""

__RCSID__ = "$Id$"

COMPONENT_NAME = 'LHCbTaskManager'

import types, os, copy
from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import sortList, fromChar
from DIRAC.TransformationSystem.Client.TaskManager import WorkflowTasks

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

class LHCbWorkflowTasks( WorkflowTasks ):
  """ A simple LHCb extension to the task manager, for now only used to set the runNumber
  """

  def __init__( self, transClient = None, logger = None, submissionClient = None, jobMonitoringClient = None,
                outputDataModule = None, opsH = None ):
    """ calls __init__ of super class
    """

    if not outputDataModule:
      outputDataModule = gConfig.getValue( "/DIRAC/VOPolicy/OutputDataModule",
                                           "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )

    if opsH:
      self.opsH = opsH
    else:
      from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
      self.opsH = Operations()

    super( LHCbWorkflowTasks, self ).__init__( outputDataModule = outputDataModule )

  #############################################################################

  def prepareTransformationTasks( self, transBody, taskDict, owner = '', ownerGroup = '', job = None ):
    """ For the moment this is just a copy/paste of the superclass method that:
        - use LHCbJob instead of Job
        - adds the runNumber information
        it would be better to split the code of the base DIRAC class, because here there's everything
    """
    if ( not owner ) or ( not ownerGroup ):
      from DIRAC.Core.Security.ProxyInfo import getProxyInfo
      res = getProxyInfo( False, False )
      if not res['OK']:
        return res
      proxyInfo = res['Value']
      owner = proxyInfo['username']
      ownerGroup = proxyInfo['group']

    if not job:
      oJob = LHCbJob( transBody )
    else:
      oJob = job( transBody )

    for taskNumber in sortList( taskDict.keys() ):
      paramsDict = taskDict[taskNumber]
      transID = paramsDict['TransformationID']
      self.log.verbose( 'Setting job owner:group to %s:%s' % ( owner, ownerGroup ) )
      oJob.setOwner( owner )
      oJob.setOwnerGroup( ownerGroup )
      transGroup = str( transID ).zfill( 8 )
      self.log.verbose( 'Adding default transformation group of %s' % ( transGroup ) )
      oJob.setJobGroup( transGroup )
      constructedName = str( transID ).zfill( 8 ) + '_' + str( taskNumber ).zfill( 8 )
      self.log.verbose( 'Setting task name to %s' % constructedName )
      oJob.setName( constructedName )
      oJob._setParamValue( 'PRODUCTION_ID', str( transID ).zfill( 8 ) )
      oJob._setParamValue( 'JOB_ID', str( taskNumber ).zfill( 8 ) )
      inputData = None

      self.log.debug( 'TransID: %s, TaskID: %s, paramsDict: %s' % ( transID, taskNumber, str( paramsDict ) ) )

      #These helper functions do the real job
      sites = self._handleDestination( paramsDict )
      if not sites:
        self.log.error( 'Could not get a list a sites', ', '.join( sites ) )
        taskDict[taskNumber]['TaskObject'] = ''
        continue
      else:
        sitesString = ', '.join( sites )
        self.log.verbose( 'Setting Site: ', sitesString )
        oJob.setDestination( sitesString )

      self._handleInputs( oJob, paramsDict )
      self._handleRest( oJob, paramsDict )

      hospitalTrans = [int( x ) for x in self.opsH.getValue( "Hospital/Transformations", [] )]
      if int( transID ) in hospitalTrans:
        self.handleHospital( oJob )

      taskDict[taskNumber]['TaskObject'] = ''
      res = self.getOutputData( {'Job':oJob._toXML(), 'TransformationID':transID,
                                 'TaskID':taskNumber, 'InputData':inputData},
                                moduleLocation = self.outputDataModule )
      if not res ['OK']:
        self.log.error( "Failed to generate output data", res['Message'] )
        continue
      for name, output in res['Value'].items():
        oJob._addJDLParameter( name, ';'.join( output ) )
      taskDict[taskNumber]['TaskObject'] = LHCbJob( oJob._toXML() )
    return S_OK( taskDict )

  #############################################################################

  def _handleDestination( self, paramsDict, getSitesForSE = None ):
    """ Handle Sites and TargetSE in the parameters
    """

    try:
      sites = ['ANY']
      if paramsDict['Site']:
        sites = fromChar( paramsDict['Site'] )
    except KeyError:
      pass

    try:
      seList = ['Unknown']
      if paramsDict['TargetSE']:
        seList = fromChar( paramsDict['TargetSE'] )
    except KeyError:
      pass

    if not seList or seList == ['Unknown']:
      return sites

    if not getSitesForSE:
      from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE

    seSites = []
    for se in seList:
      res = getSitesForSE( se )
      if not res['OK']:
        self.log.warn( 'Could not get Sites associated to SE', res['Message'] )
      else:
        thisSESites = res['Value']
        if not thisSESites:
          continue
        if seSites == []:
          seSites = copy.deepcopy( thisSESites )
        else:
          # If it is not the first SE, keep only those that are common
          for nSE in list( seSites ):
            if nSE not in thisSESites:
              seSites.remove( nSE )

    # Now we need to make the AND with the sites, if defined
    if sites == ['ANY']:
      return seSites
    else:
      # Need to get the AND
      for nSE in list( seSites ):
        if nSE not in sites:
          seSites.remove( nSE )

      return seSites


  def _handleInputs( self, oJob, paramsDict ):
    """ set job inputs (+ metadata)
    """
    try:
      if paramsDict['InputData']:
        self.log.verbose( 'Setting input data to %s' % paramsDict['InputData'] )
        self.log.verbose( 'Setting run number to %s' % str( paramsDict['RunNumber'] ) )
        oJob.setInputData( paramsDict['InputData'], runNumber = paramsDict['RunNumber'] )

        try:
          runMetadata = paramsDict['RunMetadata']
          self.log.verbose( 'Setting run metadata information to %s' % str( runMetadata ) )
          oJob.setRunMetadata( runMetadata )
        except KeyError:
          pass

    except KeyError:
      self.log.error( 'Could not found an input data or a run number' )
      raise KeyError, 'Could not found an input data or a run number'



  def _handleRest( self, oJob, paramsDict ):
    """ add as JDL parameters all the other parameters that are not for inputs or destination 
    """

    for paramName, paramValue in paramsDict.items():
      if paramName not in ( 'InputData', 'RunNumber', 'RunMetadata', 'Site', 'TargetSE' ):
        if paramValue:
          self.log.verbose( 'Setting %s to %s' % ( paramName, paramValue ) )
          oJob._addJDLParameter( paramName, paramValue )


  def _handleHospital( self, oJob ):
    """ Optional handle of hospital jobs
    """

    oJob.setType( 'Hospital' )
    oJob.setInputDataPolicy( 'download', dataScheduling = False )
    hospitalSite = self.opsH.getValue( "Hospital/HospitalSite", 'DIRAC.JobDebugger.ch' )
    oJob.setDestination( hospitalSite )
    hospitalCEs = self.opsH.getValue( "Hospital/HospitalCEs", [] )
    if hospitalCEs:
      oJob._addJDLParameter( 'GridRequiredCEs', hospitalCEs )

  #############################################################################


  def submitTaskToExternal( self, job ):
    """ Submit a task to the WMS.
        The only difference with the base DIRAC class is the use of LHCbJob instead of Job
    """
    if type( job ) in types.StringTypes:
      try:
        job = LHCbJob( job )
      except Exception, x:
        self.log.exception( "Failed to create job object", '', x )
        return S_ERROR( "Failed to create job object" )
    elif type( job ) == types.InstanceType:
      pass
    else:
      self.log.error( "No valid job description found" )
      return S_ERROR( "No valid job description found" )
    workflowFile = open( "jobDescription.xml", 'w' )
    workflowFile.write( job._toXML() )
    workflowFile.close()
    jdl = job._toJDL()
    res = self.submissionClient.submitJob( jdl )
    os.remove( "jobDescription.xml" )
    return res

  #############################################################################
