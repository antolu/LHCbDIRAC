# $HeadURL$
''' LHCb RunTestScript SAM Test Module
'''

import os
import sys

from DIRAC import S_OK, S_ERROR

from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM import ModuleBaseSAM

__RCSID__ = "$Id$"

class RunTestScript( ModuleBaseSAM ):

  def __init__( self ):
    '''
       Standard constructor for SAM Module
    '''
    ModuleBaseSAM.__init__( self )
    
    self.logFile  = 'sam-run-test-script.log'
    self.testName = 'CE-lhcb-test-script'

    #Workflow parameters for the test
    self.scriptName = ''

  def resolveInputVariables( self ):
    '''
       By convention the workflow parameters are resolved here.
    '''
    
    ModuleBaseSAM.resolveInputVariables( self )    

    if 'scriptName' in self.step_commons:
      self.scriptName = self.step_commons[ 'scriptName' ]
      if not isinstance( self.scriptName, str ):
        self.log.warn( 'Script name parameter set to non-string value %s, setting enable to False' % self.scriptName )
        self.enable = False
    else:
      self.log.warn( 'Script name not set, setting enable flag to False' )

    self.log.verbose( 'Script name is set to %s' % self.scriptName )
    return S_OK()

  def _execute( self ):
    '''
       The main execution method of the RunTestScript module.
    '''

    result = self.__checkScript()
    if not result[ 'OK' ]:
      #FIXME: this is really weird. Finalize will return S_OK ( 'Script not found' )
      #because SamResult has info level. As I understand it, should be something else.
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    self.setApplicationStatus( '%s Successful' % self.testName )
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  def __checkScript( self ):
    '''
       Checks script
    '''
    self.log.info( '>> __checkScript' )
    
    #Should fail the test in the case where the script is not locally available on the WN
    if not os.path.exists( '%s/%s' % ( os.getcwd(), self.scriptName ) ):
      result = S_ERROR( self.scriptName )
      result[ 'Description' ] = 'Script not found'
      result[ 'SamResult' ]   = 'notice'
      
      return result

    #Assume any status code is ok but report a non-zero status to the logs and report SAM notice status
    cmd = '%s %s' % ( sys.executable, self.scriptName )
    self.log.info( 'Prepended DIRAC python to script, execution command will be "%s"' % ( cmd ) )
    result = self.runCommand( 'Executing script with commmand "%s"' % cmd, cmd, check = True )
    if not result[ 'OK' ]:
      self.log.warn( '%s returned non-zero status' % ( self.scriptName ) )
      
      result[ 'Message' ]     = self.scriptName
      result[ 'Description' ] = 'Script not found'
      result[ 'SamResult' ]   = 'info'
      
      return result  
      
    return S_OK()
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF