# $HeadURL$
''' ModuleBaseSAM 
  
  Base class for LHCb SAM workflow modules. Defines several
  common utility methods.
'''

import os
import time

import DIRAC

from DIRAC                           import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id$"

class ModuleBaseSAM( object ):

  def __init__( self ):
    """ Initialize some common SAM parameters.
    """
    self.samStatus = {
                       'ok'          : '10', 
                       'info'        : '20', 
                       'notice'      : '30', 
                       'warning'     : '40', 
                       'error'       : '50', 
                       'critical'    : '60', 
                       'maintenance' : '100'
                      }
    
    self.jobID            = None
    self.testName         = None
    self.logFile          = None
    self.version          = None
    self.enable           = None
    self.jobReport        = None
    
    # Memebers injected on Workflow.execution.. so ugly !
    self.stepStatus       = None
    self.workflowStatus   = None
    self.step_commons     = None
    self.workflow_commons = None

  def setApplicationStatus( self, status ):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    if not self.testName:
      return S_ERROR( 'No SAM test name defined' )

    gLogger.verbose( 'setJobApplicationStatus(%s,%s,%s)' % ( self.jobID, status, self.testName ) )

    if 'JobReport' in self.workflow_commons:
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK( 'No reporting tool given' )
    jobStatus = self.jobReport.setApplicationStatus( status )
    if not jobStatus['OK']:
      gLogger.warn( jobStatus['Message'] )

    return jobStatus

  def getRunInfo( self ):
    """ print the basic information about CE, Host, DN, proxy, mapping
        return a dictionnary with the RUN INFO to be used later if needed
    """
    runInfo = {}
    result = self.getSAMNode()
    if not result['OK']:
      return self.finalize( 'Could not get current CE', result['Message'], 'error' )
    else:
      #self.log.info( 'Current CE is %s' % result['Value'] )
      runInfo['CE'] = result['Value']

    result = self.runCommand( 'find worker node name', 'hostname' )
    if not result['OK']:
      return self.finalize( 'Current worker node does not exist', result['Message'], 'error' )
    else:
      runInfo['WN'] = result['Value']

    result = self.runCommand( 'Checking current proxy', 'voms-proxy-info -all' )
    if not result['OK']:
      return self.finalize( 'voms-proxy-info -all', result, 'error' )
    else:
      runInfo['Proxy'] = result['Value']

    result = self.runCommand( 'Checking current user account mapping', 'id' )
    if not result['OK']:
      return self.finalize( 'id', result['Message'], 'error' )
    else:
      runInfo['identity'] = result['Value']

    result = self.runCommand( 'Checking current user account mapping', 'id -nu' )
    if not result['OK']:
      return self.finalize( 'id -nu', result['Message'], 'error' )
    else:
      runInfo['identityShort'] = result['Value']

    return runInfo

  def getSAMNode( self ):
    """In case CE isn't defined in the local config file, try to get it through
       broker info calls.
    """
    csCE = gConfig.getValue( '/LocalSite/GridCE', '' )
    if not csCE:
      gLogger.warn( 'Could not get CE from local config file in section /LocalSite/GridCE' )
    else:
      return S_OK( csCE )

    cmd = 'edg-brokerinfo getCE || glite-brokerinfo getCE'
    result = self.runCommand( 'Trying to get local CE (SAM node name)', cmd )
    if not result['OK']:
      return result

    output = result['Value'].strip()
    ce = output.split( ':' )[0]
    if not ce:
      gLogger.warn( 'Could not get CE from broker-info call:\n%s' % output )

    if 'GridRequiredCEs' in self.workflow_commons:
      ce = self.workflow_commons['GridRequiredCEs']
      gLogger.warn( 'As a last resort setting CE to %s from workflow parameters' % ce )
    else:
      _msg = 'Could not get CE from local cfg option /Resources/Computing/InProcess/GridCE'
      _msg += ' or broker-info call or workflow parameters'
      
      return S_ERROR( _msg )

    return S_OK( ce )

  def setJobParameter( self, name, value ):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    gLogger.verbose( 'setJobParameter(%s,%s,%s)' % ( self.jobID, name, value ) )

    if 'JobReport' in self.workflow_commons:
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK( 'No reporting tool given' )
    jobParam = self.jobReport.setJobParameter( str( name ), str( value ) )
    if not jobParam['OK']:
      gLogger.warn( jobParam['Message'] )

    return jobParam

  def runCommand( self, message, cmd, check = False ):
    """Wrapper around shellCall to return S_OK(stdout) or S_ERROR(message) and
       produce the SAM log files with messages and outputs. The check flag set to True
       will return S_ERROR for critical calls that should not fail.
    """
    if not self.logFile:
      return S_ERROR( 'No LogFile defined' )

    if not self.testName:
      return S_ERROR( 'No SAM test name defined' )

    if not self.version:
      return S_ERROR( 'CVS version tag is not defined' )

#    try:
    if not self.enable in [ True, False ]:
      return S_ERROR( 'Expected boolean for enable flag' )
#    except AttributeError:
#      return S_ERROR( 'Enable flag is not defined' )

    gLogger.verbose( message )
    if not os.path.exists( '%s' % ( self.logFile ) ):
      fopen = open( self.logFile, 'w' )
      _msg = 'DIRAC SAM Test: %s\nSite Name: %s\nLogFile: %s\nVersion: %s\nTest Executed On: %s [UTC]' 
      _msg = _msg % ( self.logFile, DIRAC.siteName(), self.testName, self.version, time.asctime() ) 
      header = self.getMessageString( _msg, True )
      fopen.write( header )
      fopen.close()

    if not self.enable:
      cmd = 'echo "Enable flag is False, would have executed:"\necho "%s"' % cmd

    result = shellCall( 0, cmd )
    if not result['OK']:
      return result
    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    gLogger.verbose( stdout )
    if stderr:
      gLogger.warn( stderr )

    fopen = open( self.logFile, 'a' )
    cmdHeader = self.getMessageString( 'Message: %s\nCommand: %s' % ( message, cmd ) )
    fopen.write( cmdHeader )
    fopen.write( stdout )
    gLogger.verbose( cmdHeader )
    gLogger.verbose( stdout )
    if stderr:
      gLogger.warn( stderr )
      fopen.write( stderr )
    fopen.close()
    if status:
      gLogger.info( 'Non-zero status %s while executing %s' % ( status, cmd ) )
      if check:
        return S_ERROR( stderr )
      return S_OK( stdout )
    else:
      return S_OK( stdout )

  @staticmethod
  def getMessageString( message, header = False ):
    """Return a nicely formatted string for the SAM logs.
    """
    border = ''
    limit = 0
    for line in message.split( '\n' ):
      if line:
        if len( line ) > limit:
          limit = len( line )

    maxLimit = 100
    if limit > 100:
      limit = maxLimit

    for _i in xrange( limit ):
      if header:
        border += '='
      else:
        border += '-'
    if header:
      message = '\n%s\n%s\n%s\n' % ( border, message, border )
    else:
      message = '%s\n%s\n%s\n' % ( border, message, border )
    return message

  def setSAMLogFile( self ):
    """Simple function to store the SAM log file name and test name in the
       workflow parameters.
    """
    if not self.logFile:
      return S_ERROR( 'No LogFile defined' )

    if not self.testName:
      return S_ERROR( 'No SAM test name defined' )

    if not 'SAMLogs' in self.workflow_commons:
      self.workflow_commons['SAMLogs'] = {}

    self.workflow_commons['SAMLogs'][self.testName] = self.logFile
    return S_OK()

  def writeToLog( self, message ):
    """Write to the log file with a printed message.
    """
    if not os.path.exists( '%s' % ( self.logFile ) ):
      fopen = open( self.logFile, 'w' )
      _msg = 'DIRAC SAM Test: %s\nSite Name: %s\nLogFile: %s\nVersion: %s\nTest Executed On: %s [UTC]'
      _msg = _msg % ( self.logFile, DIRAC.siteName(), self.testName, self.version, time.asctime() )
      header = self.getMessageString( _msg , True )
      fopen.write( header )
      fopen.close()

    fopen = open( self.logFile, 'a' )
    fopen.write( self.getMessageString( '%s' % ( message ) ) )
    fopen.close()
    return S_OK()

  def finalize( self, message, result, samResult ):
    """Finalize properly by setting the appropriate result at the step level
       in the workflow, errorDict is an S_ERROR() from a command that failed.
    """
    if not self.logFile:
      return S_ERROR( 'No LogFile defined' )

    if not samResult in self.samStatus:
      return S_ERROR( '%s is not a valid SAM status' % ( samResult ) )

    if not self.testName:
      return S_ERROR( 'No SAM test name defined' )

    if not self.version:
      return S_ERROR( 'CVS version tag is not defined' )

    if not os.path.exists( '%s' % ( self.logFile ) ):
      fopen = open( self.logFile, 'w' )
      _msg = 'DIRAC SAM Test: %s\nSite Name: %s\nLogFile: %s\nVersion: %s\nTest Executed On: %s [UTC]'
      _msg = _msg % ( self.logFile, DIRAC.siteName(), self.testName, self.version, time.asctime() )
      header = self.getMessageString( _msg , True )
      fopen.write( header )
      fopen.close()

    gLogger.info( '%s\n%s' % ( message, result ) )
    fopen = open( self.logFile, 'a' )
    fopen.write( self.getMessageString( '%s\n%s' % ( message, result ) ) )
    statusCode = self.samStatus[samResult]
    fopen.write( self.getMessageString( 'Exiting with SAM status %s=%s' % ( samResult, statusCode ), True ) )
    fopen.close()
    if not 'SAMResults' in self.workflow_commons:
      self.workflow_commons['SAMResults'] = {}

    self.workflow_commons['SAMResults'][self.testName] = statusCode
    if int( statusCode ) < 50:
      self.setApplicationStatus( '%s Successful (%s)' % ( self.testName, samResult ) )
      return S_OK( message )
    else:
      return S_ERROR( message )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF