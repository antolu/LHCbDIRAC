""" Extension of DIRAC Task Manager
"""

COMPONENT_NAME = 'LHCbTaskManager'

__RCSID__ = "$Id$"

import string, re, time, types, os
from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import sortList
from DIRAC.TransformationSystem.Client.TaskManager import WorkflowTasks
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

class LHCbWorkflowTasks( WorkflowTasks ):
  """ A simple LHCb extension to the task manager, for now only used to set the runNumber
  """

  def __init__( self, transClient = None, logger = None, submissionClient = None, jobMonitoringClient = None,
                outputDataModule = None ):
    """ calls __init__ of super class
    """

    if not outputDataModule:
      outputDataModule = gConfig.getValue( "/DIRAC/VOPolicy/OutputDataModule", "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )

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
      for paramName, paramValue in paramsDict.items():
        self.log.verbose( 'TransID: %s, TaskID: %s, ParamName: %s, ParamValue: %s' % ( transID, taskNumber, paramName, paramValue ) )
        if paramName == 'InputData':
          if paramValue:
            self.log.verbose( 'Setting input data to %s' % paramValue )
            self.log.verbose( 'Setting run number to %s' % str( paramsDict['RunNumber'] ) )
            oJob.setInputData( paramValue, runNumber = paramsDict['RunNumber'] )

            try:
              runMetadata = paramsDict['RunMetadata']
              self.log.verbose( 'Setting run metadata informations to %s' % str( runMetadata ) )
              oJob.setRunMetadata( runMetadata )
            except KeyError:
              pass

        elif paramName == 'Site':
          if paramValue:
            self.log.verbose( 'Setting allocated site to: %s' % ( paramValue ) )
            oJob.setDestination( paramValue )
        elif paramName == 'TargetSE' and paramsDict.get( 'Site', 'ANY' ).upper() == 'ANY':
          sites = []
          seList = paramValue.split( ',' )
          for se in seList:
            res = getSitesForSE( se )
            if res['OK']:
              sites += [site for site in res['Value'] if site not in sites]
          self.log.verbose( 'Setting allocated site from TargetSE to: %s' % ( sites ) )
          oJob.setDestination( sites )
        elif paramValue:
          self.log.verbose( 'Setting %s to %s' % ( paramName, paramValue ) )
          oJob._addJDLParameter( paramName, paramValue )

      hospitalTrans = [int( x ) for x in gConfig.getValue( "/Operations/Hospital/Transformations", [] )]
      if int( transID ) in hospitalTrans:
        hospitalSite = gConfig.getValue( "/Operations/Hospital/HospitalSite", 'DIRAC.JobDebugger.ch' )
        hospitalCEs = gConfig.getValue( "/Operations/Hospital/HospitalCEs", [] )
        oJob.setType( 'Hospital' )
        oJob.setDestination( hospitalSite )
        oJob.setInputDataPolicy( 'download', dataScheduling = False )
        if hospitalCEs:
          oJob._addJDLParameter( 'GridRequiredCEs', hospitalCEs )
      taskDict[taskNumber]['TaskObject'] = ''
      res = self.getOutputData( {'Job':oJob._toXML(), 'TransformationID':transID, 'TaskID':taskNumber, 'InputData':inputData},
                                moduleLocation = self.outputDataModule )
      if not res ['OK']:
        self.log.error( "Failed to generate output data", res['Message'] )
        continue
      for name, output in res['Value'].items():
        oJob._addJDLParameter( name, string.join( output, ';' ) )
      taskDict[taskNumber]['TaskObject'] = LHCbJob( oJob._toXML() )
    return S_OK( taskDict )

  #############################################################################

  def submitTaskToExternal( self, job ):
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
