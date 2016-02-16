
""" The job path resolution module is a VO-specific plugin that
    allows to define VO job policy in a simple way.  This allows the
    inclusion of LHCb specific WMS optimizers without compromising the
    generic nature of DIRAC.

    The arguments dictionary from the JobPathAgent includes the ClassAd
    job description and therefore decisions are made based on the existence
    of JDL parameters.
"""

from DIRAC.WorkloadManagementSystem.DB.JobDB               import JobDB
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

COMPONENT_NAME = 'LHCbJobPathResolution'

class JobPathResolution:
  """ Main class for JobPathResolution """

  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )

  def execute( self ):
    """Given the arguments from the JobPathAgent, this function resolves job optimizer
       paths according to LHCb VO policy.
    """

    if not self.arguments.has_key( 'ConfigPath' ):
      self.log.warn( 'No CS ConfigPath defined' )
      return S_ERROR( 'JobPathResoulution Failure' )

    self.log.verbose( 'Attempting to resolve job path for LHCb' )
    job = self.arguments['JobID']
    section = self.arguments['ConfigPath']
    if 'ClassAd' in self.arguments:
      classadJob = self.arguments['ClassAd']
      return self.__classAdPath( job, section, classadJob )
    elif 'JobState' in self.arguments:
      jobState = self.arguments[ 'JobState' ]
      return self.__jobStatePath( job, section, jobState )
    return S_ERROR( "No JobState or ClassAd in arguments!" )


  def __jobStatePath( self, jid, section, jobState ): 
    path = []
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    jobManifest = result[ 'Value' ]

    ancestorDepth = jobManifest.getOption( 'AncestorDepth', '' ).replace( 'Unknown', '' )
    if ancestorDepth:
      self.log.info( 'Job %s has specified ancestor depth' % ( jid ) )
      ancestors = gConfig.getValue( '%s/AncestorFiles' % section, 'AncestorFiles' )
      path.append( ancestors )

    inputData = jobManifest.getOption( "InputData", '' ).replace( 'Unknown', '' )
    if inputData:
      if not jobManifest.getOption( 'DisableDataScheduling', False ):
        self.log.info( 'Job %s has input data requirement' % ( jid ) )
        bkInputData = gConfig.getValue( '%s/BKInputData' % section, 'BKInputData' )
        path.append( 'InputData' )
        if bkInputData not in path:
          path.append( bkInputData )
      else:
        self.log.info( 'Job %s has input data requirement but scheduling via input data is disabled' % ( jid ) )
        result = JobDB().setInputData( jid, [] )
        if not result['OK']:
          self.log.error( result )
          return S_ERROR( 'Could not reset input data to null' )

    if jobManifest.getOption( 'CondDBTags', "" ):
      condDB = gConfig.getValue( '%s/CondDB' % section, 'CondDB' )
      path.append( condDB )

    if not path:
      self.log.info( 'No LHCb specific optimizers to be added' )

    return S_OK( path )
 
  def __classAdPath( self, job, section, classadJob ):
    lhcbPath = ''
    ancestorDepth = classadJob.get_expression( 'AncestorDepth' ).replace( '"', '' ).replace( 'Unknown', '' )
    if ancestorDepth:
      self.log.info( 'Job %s has specified ancestor depth' % ( job ) )
      ancestors = gConfig.getValue( section + '/AncestorFiles', 'AncestorFiles' )
      lhcbPath += ancestors + ','

    inputData = classadJob.get_expression( 'InputData' ).replace( '"', '' ).replace( 'Unknown', '' )
    if inputData and not classadJob.lookupAttribute( 'DisableDataScheduling' ):
      self.log.info( 'Job %s has input data requirement' % ( job ) )
      bkInputData = gConfig.getValue( section + '/BKInputData', 'BKInputData' )
      lhcbPath += 'InputData,'
      lhcbPath += bkInputData + ','

    if inputData and classadJob.lookupAttribute( 'DisableDataScheduling' ):
      self.log.info( 'Job %s has input data requirement but scheduling via input data is disabled' % ( job ) )
      result = JobDB().setInputData( job, [] )
      if not result['OK']:
        self.log.error( result )
        return S_ERROR( 'Could not reset input data to null' )

    if classadJob.lookupAttribute( 'CondDBTags' ):
      condDB = gConfig.getValue( section + '/CondDB', 'CondDB' )
      lhcbPath += condDB + ','

    if not lhcbPath:
      self.log.info( 'No LHCb specific optimizers to be added' )

    return S_OK( lhcbPath )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
