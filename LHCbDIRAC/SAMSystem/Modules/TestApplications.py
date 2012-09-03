# $HeadURL$
''' LHCb TestApplications SAM Module

  Corresponds to SAM tests CE-lhcb-job-*. The tests are defined in CS
  path /Operations/SAM/TestApplications/<TEST NAME> = <APP NAME>.<APP VERSION>
'''

import os
import shutil
import sys

import DIRAC

from DIRAC                      import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Interfaces.API.Dirac import Dirac

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSharedArea
from LHCbDIRAC.Core.Utilities.DetectOS                     import NativeMachine
from LHCbDIRAC.Interfaces.API.LHCbJob                      import LHCbJob
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM             import ModuleBaseSAM

__RCSID__ = "$Id$"

SAM_TEST_NAME = '' #Defined in the workflow
SAM_LOG_FILE  = ''  #Defined using workflow parameters
natOS         = NativeMachine()

class TestApplications( ModuleBaseSAM ):
  """ Test Application sSAM class """

  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )
    self.logFile         = SAM_LOG_FILE
    self.testName        = SAM_TEST_NAME
#    self.appSystemConfig = gConfig.getValue('/Operations/SAM/AppTestSystemConfig','slc4_ia32_gcc34')
    self.appSystemConfig = natOS.CMTSupportedConfig()[0]

    #Workflow parameters for the test
    self.enable         = True
    self.samTestName    = ''
    self.appNameVersion = ''
    self.appNameOptions = ''

    #Default local configuration settings
    gConfig.setOptionValue( '/LocalSite/DisableLocalJobDirectory', '1' )
    gConfig.setOptionValue( '/LocalSite/DisableLocalModeCallback', '1' )

  def resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    
    ModuleBaseSAM.resolveInputVariables( self )

    if 'samTestName' in self.step_commons:
      self.testName = self.step_commons['samTestName']

    if 'appNameVersion' in self.step_commons:
      self.appNameVersion = self.step_commons['appNameVersion']
      self.logFile = 'sam-job-%s.log' % ( self.appNameVersion.replace( '.', '-' ) )

    if 'appNameOptions' in self.step_commons:
      self.appNameOptions = self.step_commons['appNameOptions']

    self.log.verbose( 'Test Name is: %s' % self.testName )
    self.log.verbose( 'Application name and version are: %s' % self.appNameVersion )
    self.log.verbose( 'Application name and options are: %s' % self.appNameOptions )
    self.log.verbose( 'Log file name is: %s' % self.logFile )
    return S_OK()

  def execute( self ):
    """The main execution method of the TestApplications module.
    """
    self.log.info( 'Initializing ' + self.version )
    self.resolveInputVariables()
    self.setSAMLogFile()

    if not self.testName or not self.appNameVersion or not self.logFile or not self.appNameOptions:
      self.result = S_ERROR( 'No application name / version defined' )

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info( 'An error was detected in a previous step, exiting with status error.' )
      return self.finalize( 'Problem during execution', 'Failure detected in a previous step', 'error' )

    self.setApplicationStatus( 'Starting %s Test' % self.testName )

    self.log.info( 'Checking local system configuration is suitable to run the application test' )
    localArch = gConfig.getValue( '/LocalSite/Architecture', '' )
    if not localArch:
      _msg = '/LocalSite/Architecture is not defined in the local configuration'
      return self.finalize( _msg, 'Could not get /LocalSite/Architecture', 'error' )

    #must get the list of compatible platforms for this architecture
    localPlatforms = gConfig.getValue( '/Resources/Computing/OSCompatibility/%s' % localArch, [] )
    if not localPlatforms:
      _msg = 'Could not obtain compatible platforms for %s' % localArch
      return self.finalize( _msg, '/Resources/Computing/OSCompatibility/%s' % localArch, 'error' )

    if not self.appSystemConfig in localPlatforms:
      if not self.appSystemConfig in localArch:
        _msg = '%s is not in list of supported system configurations at this site: %s'
        self.log.info( _msg % ( self.appSystemConfig, ','.join( localPlatforms ) ) )
        _msg = '%s is not in list of supported system configurations at this site CE: %s\nDisabling application test.'
        self.writeToLog( _msg % ( self.appSystemConfig, ','.join( localPlatforms ) ) )
        return self.finalize( '%s Test Disabled' % self.testName, 'Status NOTICE (=30)', 'notice' )
      else:
        self.appSystemConfig = localPlatforms[0]

    options = self.__getOptions( self.appNameVersion.split( '.' )[0], 
                                 self.appNameVersion.split( '.' )[1], self.appNameOptions )
    if not options['OK']:
      return self.finalize( 'Inputs for %s %s could not be found' % ( self.appNameVersion.split( '.' )[0], 
                                                                      self.appNameVersion.split( '.' )[1] ), 
                                                                      options['Message'], 'critical' )

    sys.stdout.flush()
    result = self.__runApplication( self.appNameVersion.split( '.' )[0], 
                                    self.appNameVersion.split( '.' )[1], options['Value'] )
    sys.stdout.flush()
    if not result['OK']:
      return self.finalize( 'Failure during %s %s execution' % ( self.appNameVersion.split( '.' )[0], 
                                                                 self.appNameVersion.split( '.' )[1] ), 
                                                                 result['Message'], 'error' )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  def __getOptions( self, appName, appVersion, appOptions ):
    """Method to set the correct options for the LHCb project that will be executed.
       By convention the inputs / outputs are the system configuration + file extension.
    """
    sharedArea = getSharedArea()
    if not sharedArea or not os.path.exists( sharedArea ):
      self.log.info( 'Could not determine sharedArea for site %s:\n%s' % ( DIRAC.siteName(), sharedArea ) )
      return self.finalize( 'Could not determine shared area for site', sharedArea, 'critical' )
    else:
      self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )

    localOpts = appOptions

    #Nasty but works:
    extraOpts = ''
    if appName == 'Gauss':
      extraOpts = """ApplicationMgr().EvtMax = 2;
OutputStream("GaussTape").Output = "DATAFILE='PFN:%s/%s.sim' TYP='POOL_ROOTTREE' OPT='RECREATE'";
""" % ( os.getcwd(), self.appSystemConfig )
    elif appName == 'Boole':
      if self.enable:
        if not os.path.exists( '%s.sim' % self.appSystemConfig ):
          return S_ERROR( 'No input file %s.sim found for Boole' % ( self.appSystemConfig ) )
      extraOpts = """#Boole().useSpillover=False;
EventSelector().Input = ["DATAFILE='PFN:%s/%s.sim' TYP='POOL_ROOTTREE' OPT='READ'"];
OutputStream("DigiWriter").Output = "DATAFILE='PFN:%s/%s.digi' TYP='POOL_ROOTTREE' OPT='REC'";
""" % ( os.getcwd(), self.appSystemConfig, os.getcwd(), self.appSystemConfig )
    elif appName == 'Brunel':
      if self.enable:
        if not os.path.exists( '%s.digi' % self.appSystemConfig ):
          return S_ERROR( 'No input file %s.digi found for Brunel' % ( self.appSystemConfig ) )
      extraOpts = """EventSelector().Input = ["DATAFILE='PFN:%s/%s.digi' TYP='POOL_ROOTTREE' OPT='READ'"];
OutputStream("DstWriter").Output = "DATAFILE='PFN:%s/%s.dst' TYP='POOL_ROOTTREE' OPT='REC'";
""" % ( os.getcwd(), self.appSystemConfig, os.getcwd(), self.appSystemConfig )
    elif appName == 'DaVinci':
      if self.enable:
        if not os.path.exists( '%s.dst' % self.appSystemConfig ):
          return S_ERROR( 'No input file %s.dst found for DaVinci' % ( self.appSystemConfig ) )
      extraOpts = """EventSelector().Input = ["DATAFILE='PFN:%s/%s.dst' TYP='POOL_ROOTTREE' OPT='READ'"];
""" % ( os.getcwd(), self.appSystemConfig )

    newOpts = '%s-Extra.py' % ( appName )
    self.log.verbose( 'Adding extra options for %s %s:\n%s' % ( appName, appVersion, extraOpts ) )
    fopen = open( newOpts, 'w' )
    if appName == 'DaVinci':
      fopen.write( '#\n# Options added by TestApplications for DIRAC SAM test %s\n#\nfrom %s.Configuration import *\n' % ( self.testName, 'Gaudi' ) )
    else:
      fopen.write( '#\n# Options added by TestApplications for DIRAC SAM test %s\n#\nfrom %s.Configuration import *\n' % ( self.testName, appName ) )
    fopen.write( extraOpts )
    fopen.close()
    return S_OK( [localOpts, newOpts] )

  def __runApplication( self, appName, appVersion, options ):
    """Method to run a test job locally.
    """
    result = S_OK()
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    try:
      j = LHCbJob( stdout = self.logFile.replace( 'log', 'stdout' ), stderr = self.logFile.replace( 'log', 'stderr' ) )
      j.setSystemConfig( self.appSystemConfig )
      j.setApplication( appName, appVersion, options, logFile = self.logFile, events = 2 )
      j.setName( '%s%sSAMTest' % ( appName, appVersion ) )
      j.setLogLevel( Operations().getValue( 'SAM/LogLevel', 'verbose' ) )
      j.setPlatform( Operations().getValue( 'SAM/Platform', 'gLite' ) )
      self.log.verbose( 'Job JDL is:\n%s' % ( j._toJDL() ) )
      dirac = Dirac()
      if self.enable:
        result = dirac.submit( j, mode = 'local' )
        self.log.info( '%s %s execution result: %s' % ( appName, appVersion, result ) )
      #Correct the log file name since it will have Step1_ prepended.
      if os.path.exists( 'Step1_%s' % self.logFile ):
        shutil.move( 'Step1_%s' % self.logFile, self.logFile )
    except SystemError, x:
      self.log.warn( 'Problem during %s %s execution: "%s"' % ( appName, appVersion, x ) )
      return S_ERROR( str( x ) )
    return result

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF