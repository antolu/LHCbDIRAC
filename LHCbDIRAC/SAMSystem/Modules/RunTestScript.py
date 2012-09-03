# $HeadURL$
''' LHCb RunTestScript SAM Test Module
'''

import os
import sys

from DIRAC import S_OK, S_ERROR, gLogger

from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM import ModuleBaseSAM

__RCSID__ = "$Id$"

SAM_TEST_NAME = 'CE-lhcb-test-script'
SAM_LOG_FILE  = 'sam-run-test-script.log'
SAM_LOCK_NAME = 'DIRAC-SAM-Test-Script'

class RunTestScript( ModuleBaseSAM ):

  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )
    self.runinfo  = {}
    self.logFile  = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.lockFile = SAM_LOCK_NAME
    self.result   = S_ERROR()

    #Workflow parameters for the test
    self.enable     = True
    self.scriptName = ''

  def resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    
    ModuleBaseSAM.resolveInputVariables( self )    

    if 'scriptName' in self.step_commons:
      self.scriptName = self.step_commons['scriptName']
      if not type( self.scriptName ) == type( " " ):
        self.log.warn( 'Script name parameter set to non-string value %s, setting enable to False' % self.scriptName )
        self.enable = False
    else:
      self.log.warn( 'Script name not set, setting enable flag to False' )

    self.log.verbose( 'Script name is set to %s' % self.scriptName )
    return S_OK()

  def execute( self ):
    """The main execution method of the RunTestScript module.
    """
    self.log.info( 'Initializing ' + self.version )
    self.resolveInputVariables()
    self.setSAMLogFile()
    self.result = S_OK()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info( 'An error was detected in a previous step, exiting with status error.' )
      return self.finalize( 'Problem during execution', 'Failure detected in a previous step', 'error' )

    self.setApplicationStatus( 'Starting %s Test' % self.testName )
    self.runinfo = self.getRunInfo()

    #Should fail the test in the case where the script is not locally available on the WN
    if not os.path.exists( '%s/%s' % ( os.getcwd(), self.scriptName ) ):
      return self.finalize( 'Script not found', '%s' % ( self.scriptName ), 'notice' )

    #Assume any status code is ok but report a non-zero status to the logs and report SAM notice status
    cmd = '%s %s' % ( sys.executable, self.scriptName )
    self.log.info( 'Prepended DIRAC python to script, execution command will be "%s"' % ( cmd ) )
    result = self.runCommand( 'Executing script with commmand "%s"' % cmd, cmd, check = True )
    if not result['OK']:
      self.log.warn( '%s returned non-zero status' % ( self.scriptName ) )
      return self.finalize( 'Script not found', '%s' % ( self.scriptName ), 'info' )

    self.setApplicationStatus( '%s Successful' % self.testName )
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF