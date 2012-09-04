# $HeadURL$
''' LHCb TestApplications SAM Module

  Corresponds to SAM tests CE-lhcb-job-*. The tests are defined in CS
  path /Operations/SAM/TestApplications/<TEST NAME> = <APP NAME>.<APP VERSION>
'''

import os
import shutil
import sys

import DIRAC

from DIRAC                                               import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Interfaces.API.Dirac                          import Dirac

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSharedArea
from LHCbDIRAC.Core.Utilities.DetectOS                     import NativeMachine
from LHCbDIRAC.Interfaces.API.LHCbJob                      import LHCbJob
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM             import ModuleBaseSAM

__RCSID__ = "$Id$"

class TestApplications( ModuleBaseSAM ):
  '''
     Test Application SAM class
  '''

  def __init__( self ):
    '''
       Standard constructor for SAM Module
    '''
    
    ModuleBaseSAM.__init__( self )
    
    self.logFile         = ''
    self.testName        = ''
#    self.appSystemConfig = gConfig.getValue('/Operations/SAM/AppTestSystemConfig','slc4_ia32_gcc34')
    self.appSystemConfig = NativeMachine().CMTSupportedConfig()[ 0 ]

    #Workflow parameters for the test
    self.appNameVersion = ''
    self.appNameOptions = ''

    #Default local configuration settings
    gConfig.setOptionValue( '/LocalSite/DisableLocalJobDirectory', '1' )
    gConfig.setOptionValue( '/LocalSite/DisableLocalModeCallback', '1' )

  def resolveInputVariables( self ):
    '''
       By convention the workflow parameters are resolved here.
    '''
    
    ModuleBaseSAM.resolveInputVariables( self )

    if 'samTestName' in self.step_commons:
      self.testName = self.step_commons[ 'samTestName' ]

    if 'appNameVersion' in self.step_commons:
      self.appNameVersion = self.step_commons[ 'appNameVersion' ]
      self.logFile = 'sam-job-%s.log' % ( self.appNameVersion.replace( '.', '-' ) )

    if 'appNameOptions' in self.step_commons:
      self.appNameOptions = self.step_commons[ 'appNameOptions' ]

    self.log.verbose( 'Test Name is: %s' % self.testName )
    self.log.verbose( 'Application name and version are: %s' % self.appNameVersion )
    self.log.verbose( 'Application name and options are: %s' % self.appNameOptions )
    self.log.verbose( 'Log file name is: %s' % self.logFile )
    return S_OK()

  def _execute( self ):
    '''
       The main execution method of the TestApplications module.
       Checks:
         - appName, appVersion
         - gets options
         - runs application
    '''

    if not self.testName or not self.appNameVersion or not self.logFile or not self.appNameOptions:
      return self.finalize( 'No application name / version defined', 'Unknown', 'error' )

    result = self.__checkPlatforms()
    if not result[ 'OK' ]:
      self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    options = self.__getOptions( self.appNameVersion.split( '.' )[0], 
                                 self.appNameVersion.split( '.' )[1] )
    if not options[ 'OK' ] :
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    sys.stdout.flush()
    result = self.__runApplication( self.appNameVersion.split( '.' )[0], 
                                    self.appNameVersion.split( '.' )[1], 
                                    options[ 'Value' ] )
    sys.stdout.flush()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  ##############################################################################
  # Protected methods

  def __checkPlatforms( self ):
    '''
       Checks local architecture and compares it with supported platforms
    '''
    
    self.log.info( 'Checking local system configuration is suitable to run the application test' )
    localArch = gConfig.getValue( '/LocalSite/Architecture', '' )
    if not localArch:
      result = S_ERROR( 'Could not get /LocalSite/Architecture' )
      result[ 'Description' ] = '/LocalSite/Architecture is not defined in the local configuration'
      result[ 'SamResult' ]   = 'error'
      return result

    #must get the list of compatible platforms for this architecture
    localPlatforms = gConfig.getValue( '/Resources/Computing/OSCompatibility/%s' % localArch, [] )
    if not localPlatforms:
      result = S_ERROR( '/Resources/Computing/OSCompatibility/%s' % localArch )
      result[ 'Description' ] = 'Could not obtain compatible platforms for %s' % localArch
      result[ 'SamResult' ]   = 'error'
      return result

    if not self.appSystemConfig in localPlatforms:
      if not self.appSystemConfig in localArch:
        _msg = '%s is not in list of supported system configurations at this site: %s'
        self.log.info( _msg % ( self.appSystemConfig, ','.join( localPlatforms ) ) )
        _msg = '%s is not in list of supported system configurations at this site CE: %s\nDisabling application test.'
        self.writeToLog( _msg % ( self.appSystemConfig, ','.join( localPlatforms ) ) )
        
        result = S_ERROR( 'Status NOTICE (=30)' )
        result[ 'Description' ] = '%s Test Disabled' % self.testName
        result[ 'SamResult' ]   = 'notice'        
        return result
        
      else:
        self.appSystemConfig = localPlatforms[0]
      
    return S_OK()

  def __getOptions( self, appName, appVersion ):
    '''
       Method to set the correct options for the LHCb project that will be executed.
       By convention the inputs / outputs are the system configuration + file extension.
    '''
    
    sharedArea = getSharedArea()
    if not sharedArea or not os.path.exists( sharedArea ):
      self.log.info( 'Could not determine sharedArea for site %s:\n%s' % ( DIRAC.siteName(), sharedArea ) )
      
      result                  = S_ERROR( sharedArea )
      result[ 'Description' ] = 'Could not determine shared area for site'
      result[ 'SamResult' ]   = 'critical'
      
      return result

    else:
      self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )

    extraOpts = ''
    
    result = None
    
    if appName == 'Gauss':
      extraOpts = 'ApplicationMgr().EvtMax = 2;\n'
      extraOpts += '''OutputStream("GaussTape").Output = "DATAFILE='PFN:%s/%s.sim' '''
      extraOpts += '''TYP='POOL_ROOTTREE' OPT='RECREATE'";'''
      extraOpts = extraOpts % ( os.getcwd(), self.appSystemConfig )

    elif appName == 'Boole':
      if self.enable:
        if not os.path.exists( '%s.sim' % self.appSystemConfig ):         
          result = S_ERROR( 'No input file %s.sim found for Boole' % ( self.appSystemConfig ) )

      extraOpts = '#Boole().useSpillover=False;\n'
      extraOpts += '''EventSelector().Input = ["DATAFILE='PFN:%s/%s.sim' TYP='POOL_ROOTTREE' OPT='READ'"];\n'''
      extraOpts += '''OutputStream("DigiWriter").Output = "DATAFILE='PFN:%s/%s.digi' TYP='POOL_ROOTTREE' OPT='REC';'''
      extraOpts = extraOpts % ( os.getcwd(), self.appSystemConfig, os.getcwd(), self.appSystemConfig )

    elif appName == 'Brunel':
      if self.enable:
        if not os.path.exists( '%s.digi' % self.appSystemConfig ):
          result = S_ERROR( 'No input file %s.digi found for Brunel' % ( self.appSystemConfig ) )
      
      extraOpts = '''EventSelector().Input = ["DATAFILE='PFN:%s/%s.digi' TYP='POOL_ROOTTREE' OPT='READ'"];\n'''
      extraOpts += '''OutputStream("DstWriter").Output = "DATAFILE='PFN:%s/%s.dst' TYP='POOL_ROOTTREE' OPT='REC'";'''
      extraOpts = extraOpts % ( os.getcwd(), self.appSystemConfig, os.getcwd(), self.appSystemConfig )
    
    elif appName == 'DaVinci':
      if self.enable:
        if not os.path.exists( '%s.dst' % self.appSystemConfig ):
          result = S_ERROR( 'No input file %s.dst found for DaVinci' % ( self.appSystemConfig ) )
      
      extraOpts = '''EventSelector().Input = ["DATAFILE='PFN:%s/%s.dst' TYP='POOL_ROOTTREE' OPT='READ'"];'''
      extraOpts = extraOpts % ( os.getcwd(), self.appSystemConfig )
    
      # This was done on purpose, I simply do not know now why  
      appName = 'Gaudi'

    if result is not None:  
      result[ 'Description' ] = 'Inputs for %s %s could not be found' % ( self.appNameVersion.split( '.' )[0], 
                                                                          self.appNameVersion.split( '.' )[1] ) 
      result[ 'SamResult' ]   = 'critical'
      return result
       
    newOpts = '%s-Extra.py' % ( appName )
    self.log.verbose( 'Adding extra options for %s %s:\n%s' % ( appName, appVersion, extraOpts ) )
    fopen = open( newOpts, 'w' )
    
    _msg = '#\n# Options added by TestApplications for DIRAC SAM test %s\n#\nfrom %s.Configuration import *\n' 

    fopen.write( _msg % ( self.testName, appName ) )
    fopen.write( extraOpts )
    fopen.close()
    
    return S_OK( newOpts )

  def __runApplication( self, appName, appVersion, options ):
    '''
       Method to run a test job locally.
    '''
    
    result = S_OK()
    dirac  = Dirac()
    
    options = [ self.appNameOptions, options ]
    
    try:
    
      j = LHCbJob( stdout = self.logFile.replace( 'log', 'stdout' ), 
                   stderr = self.logFile.replace( 'log', 'stderr' ) )
      j.setSystemConfig( self.appSystemConfig )
      j.setApplication( appName, appVersion, options, logFile = self.logFile, events = 2 )
      j.setName( '%s%sSAMTest' % ( appName, appVersion ) )
      j.setLogLevel( Operations().getValue( 'SAM/LogLevel', 'verbose' ) )
      j.setPlatform( Operations().getValue( 'SAM/Platform', 'gLite' ) )
      
      self.log.verbose( 'Job JDL is:\n%s' % ( j._toJDL() ) )
      
      if self.enable:
        result = dirac.submit( j, mode = 'local' )
        self.log.info( '%s %s execution result: %s' % ( appName, appVersion, result ) )
        
      #Correct the log file name since it will have Step1_ prepended.
      if os.path.exists( 'Step1_%s' % self.logFile ):
        shutil.move( 'Step1_%s' % self.logFile, self.logFile )
        
    except SystemError, x:
      
      self.log.warn( 'Problem during %s %s execution: "%s"' % ( appName, appVersion, x ) )
      result = S_ERROR( str( x ) )
      result[ 'Description' ] = ' Failure during %s %s execution' % ( appName, appVersion ) 
      result[ 'SamResult' ]   = 'error'    
    
    return result

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF