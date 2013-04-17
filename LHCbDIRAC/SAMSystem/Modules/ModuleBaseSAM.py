''' ModuleBaseSAM 
  
  Base class for LHCb SAM workflow modules. Defines several
  common utility methods.
'''

#import os
#import time
#
#import DIRAC
#
#from DIRAC                           import S_OK, S_ERROR, gLogger, gConfig, siteName
#from DIRAC.Core.Utilities.Subprocess import shellCall
#
#from LHCbDIRAC.SAMSystem.Utilities   import Utils
#
#__RCSID__ = "$Id$"
#
#class ModuleBaseSAM( object ):
#
#  def __init__( self ):
#    ''' 
#      Initialize some common SAM parameters.
#    '''
#    
#    self.nagiosStatus = { 
#                          'ok'      : 0,
#                          'warning' : 1,
#                          'error'   : 2
#                        }
#    
#    self.log              = gLogger.getSubLogger( self.__class__.__name__ )
#    self.version          = __RCSID__
#    self.jobID            = None
#    if 'JOBID' in os.environ:
#      self.jobID = os.environ[ 'JOBID' ]
#    self.enable           = True 
#
#    self.runInfo          = {}
#    self.sharedArea       = '' 
#          
#    self.testName         = self.__class__.__name__
#    self.logFile          = '%s.log' % self.testName
#    self.jobReport        = None
#    
#    # Memebers injected on Workflow.execution.. so ugly !
#    self.stepStatus       = {}
#    self.workflowStatus   = {}
#    self.step_commons     = {}
#    self.workflow_commons = {}
#
#  def resolveInputVariables( self ):
#    ''' 
#      By convention the workflow parameters are resolved here.
#    '''
#    
#    if 'enable' in self.step_commons:
#      self.enable = self.step_commons[ 'enable' ]
#      if not isinstance( self.enable, bool ):
#        self.log.warn( 'Enable flag set to non-boolean value %s, setting to False' % self.enable )
#        self.enable = False
#    
#    if 'JobReport' in self.workflow_commons:
#      self.jobReport = self.workflow_commons[ 'JobReport' ]
#    else:
#      self.log.warn( 'JobReport tool not given' )  
#    
#    self.runInfo    = self.workflow_commons.get( 'runInfo', {} )
#    self.sharedArea = self.workflow_commons.get( 'sharedArea', '' )
#    
#    self.log.verbose( 'enable = %s' % self.enable )
#    self.log.verbose( 'jobReport = %s' % self.jobReport )
#    self.log.verbose( 'runInfo = \n%s' % '\n'.join( [ '%s = "%s"' % ( k, v ) for k, v in self.runInfo.iteritems() ] ) )
#    self.log.verbose( 'sharedArea = %s' % self.sharedArea )
#    return S_OK()    
#
#  def setSAMLogFile( self ):
#    '''
#       Simple function to store the SAM log file name and test name in the
#       workflow parameters.
#    '''
#    
#    if not self.logFile:
#      return S_ERROR( 'No LogFile defined' )
#
#    if not self.testName:
#      return S_ERROR( 'No SAM test name defined' )
#
#    if not 'SAMLogs' in self.workflow_commons:
#      self.workflow_commons[ 'SAMLogs' ] = {}
#    self.workflow_commons[ 'SAMLogs' ][ self.testName ] = self.logFile
#    
#    self.log.verbose( '%s = %s' % ( self.testName, self.logFile ) ) 
#    
#    return S_OK()
#
#  def getRunInfo( self ):
#    '''
#       Get the basic information about CE, Host, DN, proxy, mapping
#       return a dictionary with the RUN INFO to be used later if needed
#    '''
#
#    # The key is on workflow_commons, which means it has also copied over by
#    # resolveInputVariables.
#    if 'runInfo' in self.workflow_commons:
#      self.log.verbose( "RunInfo is already on workflow_commons" )
#      self.log.debug( self.workflow_commons[ 'runInfo' ] )
#      return S_OK()
#      
#    runInfo = {}
#        
#    result = self._getSAMNode()
#    if not result[ 'OK' ]:
#      result[ 'SamResult'  ]  = 'error'
#      result[ 'Description' ] = 'Could not get current CE'
#      return result       
#    runInfo[ 'CE' ] = result[ 'Value' ]
#
#    result = self.runCommand( 'Find worker node name', 'hostname' )
#    if not result[ 'OK' ]:
#      result[ 'SamResult'  ]  = 'error'
#      result[ 'Description' ] = 'Current worker node does not exist'
#      return result
#    runInfo[ 'WN' ] = result[ 'Value' ]
#
#    result = self.runCommand( 'Checking current proxy', 'voms-proxy-info -all' )
#    if not result[ 'OK' ]:
#      result[ 'SamResult'  ]  = 'error'
#      result[ 'Description' ] = 'voms-proxy-info -all'
#      return result
#    runInfo[ 'Proxy' ] = result[ 'Value' ]
#
#    result = self.runCommand( 'Checking current user account mapping', 'id' )
#    if not result[ 'OK' ]:
#      result[ 'SamResult'  ]  = 'error'
#      result[ 'Description' ] = 'id'
#      return result
#    runInfo[ 'identity' ] = result[ 'Value' ]
#
#    result = self.runCommand( 'Checking current user account mapping', 'id -nu' )
#    if not result[ 'OK' ]:
#      result[ 'SamResult'  ]  = 'error'
#      result[ 'Description' ] = 'id -nu'
#      return result
#    runInfo[ 'identityShort' ] = result[ 'Value' ]
#
#    self.runInfo                       = runInfo
#    self.workflow_commons[ 'runInfo' ] = runInfo
#    #self.log.info( "Pushing runInfo to workflow_commons: \n %s" % runInfo )
#    self.log.info( "Pushing runInfo to workflow_commons" )
#    map( self.log.info, [ '%s = "%s"' % ( k, v ) for k, v in runInfo.iteritems() ] )
#
#    return S_OK( runInfo )
#
#  def getSharedArea( self ):
#    '''
#       Gets sharedArea
#    '''
#    
#    # The key is on workflow_commons, which means it has also copied over by
#    # resolveInputVariables.
#    if 'sharedArea' in self.workflow_commons:
#      self.log.verbose( "sharedArea is already on workflow_commons" )
#      self.log.debug( self.workflow_commons[ 'sharedArea' ] )
#      return S_OK()
#    
#    sharedArea = Utils.checkSharedArea( self.log )
#    if not sharedArea[ 'OK' ]:
#      return sharedArea
#    sharedArea = sharedArea[ 'Value' ]
#    
#    # This method always returns S_OK !
#    # Not needed any longer
##    newSharedArea = Utils.fixWritableVolume( sharedArea, self.log )[ 'Value' ]
##    if newSharedArea != sharedArea:
##      _msg = 'Changing path to shared area writeable volume at %s:\n%s => %s'
##      self.writeToLog( _msg % ( siteName(), sharedArea, newSharedArea ) )
##      self.log.info( _msg )
##      sharedArea = newSharedArea            
#
#    self.sharedArea                       = sharedArea
#    self.workflow_commons[ 'sharedArea' ] = sharedArea
#    self.log.info( "Pushing sharedArea to workflow_commons: %s" % sharedArea )
#    
#    return S_OK( sharedArea )
#
#  def setApplicationStatus( self, status ):
#    '''
#       Wraps around setJobApplicationStatus of state update client
#    '''
#    if not self.jobID:
#      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission
#
#    self.log.verbose( 'setJobApplicationStatus(%s,%s,%s)' % ( self.jobID, status, self.testName ) )
#
#    if not self.jobReport:
#      return S_OK( 'No reporting tool given' )
#    
#    jobStatus = self.jobReport.setApplicationStatus( status )
#    if not jobStatus[ 'OK' ]:
#      self.log.warn( jobStatus[ 'Message' ] )
#
#    return jobStatus
#
#  def execute( self ):
#    ''' Main execution method for the ModuleBaseSAM. To be extended on the inherited
#        classes. 
#    '''
#
#    self.log.info( '=' * 80 )
#    self.log.info( self.__class__.__name__ )
#    self.log.info( '=' * 80 )
#
#    # Check Status of Workflow
#    self.log.verbose( '*' * 80 )
#    self.log.verbose( '<Workflow and step Status>' )
#    if not self.workflowStatus[ 'OK' ] or not self.stepStatus[ 'OK' ]:
#      self.log.error( "An error was detected in a previous step, exiting with status error." )
#      self.log.error( "WorkflowStatus = %s" % self.workflowStatus[ 'OK' ] )
#      self.log.error( "StepStatus = %s" % self.stepStatus[ 'OK' ] )
#      return self.finalize( 'Problem during execution', 'Failure detected in a previous step', 'error' )
#    else:
#      self.log.verbose( 'WORKFLOW :%s' % self.workflowStatus[ 'OK' ] )
#      self.log.verbose( 'STEP     :%s' % self.stepStatus[ 'OK' ] )
#    
#    self.log.verbose( '*' * 80 )
#    self.log.verbose( '<setSAMLogFile>' )   
#    logFile = self.setSAMLogFile()
#    if not logFile[ 'OK' ]:
#      self.log.error( logFile[ 'Message' ] )
#      return self.finalize( 'Problem during execution', logFile[ 'Message' ], 'error' )
#    
#    self.log.verbose( '*' * 80 )
#    self.log.verbose( '<resolveInputVariables>' )
#    self.resolveInputVariables()
#
#    self.log.verbose( '*' * 80 )
#    self.log.verbose( '<runInfo>' )    
#    runInfo = self.getRunInfo()
#    if not runInfo[ 'OK' ]:
#      self.log.info( 'Error occurred while getting run Info' )
#      return self.finalize( runInfo[ 'Description' ], runInfo[ 'Message' ], runInfo[ 'SamResult' ] )
#    self.log.verbose( '*' * 80 )
#    self.log.verbose( '<sharedArea>' )
#    sharedArea = self.getSharedArea()
#    if not sharedArea[ 'OK' ]:
#      self.log.info( 'Error occurred while getting sharedArea' )
#      return self.finalize( sharedArea[ 'Description' ], sharedArea[ 'Message' ], sharedArea[ 'SamResult' ] )
#      
#    self.log.verbose( '*' * 80 )
#    self.log.verbose( '<STARTINGMODULE>' * 5 )    
#    self.setApplicationStatus( 'Starting %s Test' % self.testName )
#    
#    self.log.debug( "<STEP_COMMONS>" )
#    self.log.debug( '\n'.join( [ "%s = %s" % ( k, v ) for k, v in self.step_commons.iteritems() ] ) )
#    self.log.debug( "</STEP_COMMONS>" )
#    
#    self.log.debug( "<WORKFLOW_COMMONS>" )
#    self.log.debug( '\n'.join( [ "%s = %s" % ( k, v ) for k, v in self.workflow_commons.iteritems() ] ) )
#    self.log.debug( "</WORKFLOW_COMMONS>" )
#    
#    result = self._execute()
#    
#    self.log.verbose( '</ENDING_MODULE>' * 5 )
#    
#    self.log.info( '=' * 80 )
#    self.log.info( '#' * 80 )
#    self.log.info( '=' * 80 )
#    
#    return result
#
#  def finalize( self, message, result, nagiosResult ):
#    '''
#       Finalize properly by setting the appropriate result at the step level
#       in the workflow, errorDict is an S_ERROR() from a command that failed.
#    '''
#    
#    self.log.info( "<FINALIZE>" * 8 )
#    
#    if not nagiosResult in self.nagiosStatus:
#      return S_ERROR( '%s is not a valid NAGIOS status' % nagiosResult )
#    statusCode = self.nagiosStatus[ nagiosResult ]
#
#    self.writeToLog( '%s\n%s' % ( message, result ) )
#
#    self.log.info( '%s\n%s' % ( message, result ) )
#    fopen = open( self.logFile, 'a' )
#    
#    fopen.write( 'Exiting with NAGIOS status(code): %s(%s)\n' % ( nagiosResult, statusCode ) )
#    fopen.write( '=' * 92 )
#    
#    fopen.close()
#    
#    if not 'SAMResults' in self.workflow_commons:
#      self.workflow_commons[ 'SAMResults' ] = {}
#
#    self.workflow_commons[ 'SAMResults' ][ self.testName ] = statusCode
#    if statusCode == 0:
#      self.setApplicationStatus( '%s Successful (%s)' % ( self.testName, nagiosResult ) )
#      return S_OK( message )
#    
#    return S_ERROR( message )
#
#  ##############################################################################
#  # Private methods
#
#  def _execute( self ):
#    '''
#      Method to be overwritten by extended classes
#    ''' 
#    return S_OK( self.version ) 
#
#  def _getSAMNode( self ):
#    '''
#       In case CE isn't defined in the local config file, try to get it through
#       broker info calls.
#    '''
#    
#    self.log.info( '>> _getSAMNode' )
#    
#    csCE = gConfig.getValue( '/LocalSite/GridCE', '' )
#    if not csCE:
#      self.log.warn( 'Could not get CE from local config file in section /LocalSite/GridCE' )
#    else:
#      return S_OK( csCE )
#
#    cmd = 'edg-brokerinfo getCE || glite-brokerinfo getCE'
#    result = self.runCommand( 'Trying to get local CE (SAM node name)', cmd )
#    if not result[ 'OK' ]:
#      return result
#
#    output = result[ 'Value' ].strip()
#    ce     = output.split( ':' )[0]
#    if not ce:
#      self.log.warn( 'Could not get CE from broker-info call:\n%s' % output )
#    else:
#      return S_OK( ce )
#
#    if 'GridRequiredCEs' in self.workflow_commons:
#      ce = self.workflow_commons[ 'GridRequiredCEs' ]
#      self.log.warn( 'As a last resort setting CE to %s from workflow parameters' % ce )
#    else:
#      _msg = 'Could not get CE from local cfg option /Resources/Computing/InProcess/GridCE'
#      _msg += ' or broker-info call or workflow parameters'
#      
#      return S_ERROR( _msg )
#
#    return S_OK( ce )
#
#  def setJobParameter( self, name, value ):
#    """Wraps around setJobParameter of state update client
#    """
#    if not self.jobID:
#      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission
#
#    self.log.verbose( 'setJobParameter(%s,%s,%s)' % ( self.jobID, name, value ) )
#
#    if 'JobReport' in self.workflow_commons:
#      self.jobReport = self.workflow_commons[ 'JobReport' ]
#
#    if not self.jobReport:
#      return S_OK( 'No reporting tool given' )
#    jobParam = self.jobReport.setJobParameter( str( name ), str( value ) )
#    if not jobParam['OK']:
#      self.log.warn( jobParam['Message'] )
#
#    return jobParam
#
#  def runCommand( self, message, cmd, check = False ):
#    '''
#       Wrapper around shellCall to return S_OK(stdout) or S_ERROR(message) and
#       produce the SAM log files with messages and outputs. The check flag set to True
#       will return S_ERROR for critical calls that should not fail.
#    '''
#
#    self.log.info( '>> runCommand')
#    self.log.verbose( 'Message : %s' % message )
#    if not self.enable:
#      cmd = 'echo "Enable flag is False, would have executed:"\necho "%s"' % cmd
#    self.log.verbose( 'cmd : %s' % cmd )
#    
#    self.writeToLog( 'Message: %s\nCommand: %s' % ( message, cmd ) )
#    
#    result = shellCall( 0, cmd )
#    if not result[ 'OK' ]:
#      return result
#    
#    status = result[ 'Value' ][ 0 ]
#    stdout = result[ 'Value' ][ 1 ]
#    stderr = result[ 'Value' ][ 2 ]
#    
#    fopen = open( self.logFile, 'a' )
#    
#    self.log.verbose( 'stdout:%s' % stdout )
#    fopen.write( stdout )
#    if stderr:
#      self.log.warn( 'stderr:%s' % stderr )
#      fopen.write( stderr )
#      
#    fopen.close()
#    
#    if status:
#      self.log.info( 'Non-zero status %s while executing %s' % ( status, cmd ) )
#      if check:
#        return S_ERROR( stderr )
#      
#    return S_OK( stdout )
#
#  def writeToLog( self, message ):
#    '''
#      Write to the log file with a printed message, if the log file does not exits,
#      it creates a new one.
#    '''
#    
#    if not os.path.exists( self.logFile ):
#    
#      fopen = open( self.logFile, 'w' )
#      _msg = 'DIRAC SAM Test: %s\nSite Name: %s\nLogFile: %s\nVersion: %s\nTest Executed On: %s [UTC]'
#      _msg = _msg % ( self.logFile, DIRAC.siteName(), self.testName, self.version, time.asctime() )
#      header = Utils.getMessageString( _msg , True )
#      fopen.write( header )
#      fopen.close()
#
#    fopen = open( self.logFile, 'a' )
#    fopen.write( Utils.getMessageString( message ) )
#    fopen.close()
#    
#    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF