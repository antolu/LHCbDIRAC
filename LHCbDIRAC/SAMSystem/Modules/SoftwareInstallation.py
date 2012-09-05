# $HeadURL$
''' LHCb SoftwareInstallation SAM Test Module

  Corresponds to SAM test CE-lhcb-install, utilizes the SoftwareManagementAgent
  to perform the installation of LHCb software in site shared areas. Deprecated
  software is also removed during this phase.

'''

import os
import re
import shutil
import sys
import urllib

import DIRAC

from DIRAC import S_OK, S_ERROR, gConfig

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSharedArea, installApplication
from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation import removeApplication, createSharedArea
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM             import ModuleBaseSAM

__RCSID__ = "$Id$"

class SoftwareInstallation( ModuleBaseSAM ):
  """ SoftwareInstallation SAM class """

  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )
    
    self.logFile  = 'sam-install.log'
    self.testName = 'CE-lhcb-install'

    #Workflow parameters for the test
    self.purgeSharedArea   = False
    self.installProjectURL = None

  def resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    
    ModuleBaseSAM.resolveInputVariables( self )
    
    if 'purgeSharedAreaFlag' in self.step_commons:
      self.purgeSharedArea = self.step_commons['purgeSharedAreaFlag']
      if not type( self.purgeSharedArea ) == type( True ):
        self.log.warn( 'Purge shared area flag set to non-boolean value %s, setting to False' % self.purgeSharedArea )
        self.enable = False

    if 'installProjectURL' in self.step_commons:
      self.installProjectURL = self.step_commons['installProjectURL']
      if not type( self.installProjectURL ) == type( " " ) or not self.installProjectURL:
        self.log.warn( 'Install project URL not set to non-zero string parameter, setting to None' )
        self.installProjectURL = None

    self.log.verbose( 'Purge shared area flag set to %s' % self.purgeSharedArea )
    self.log.verbose( 'Install project URL set to %s' % ( self.installProjectURL ) )
    return S_OK()

  def _execute( self ):
    """The main execution method of the SoftwareInstallation module.
    """

    result = self.__checkSAMResults()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    if not self.enable:
      return self.finalize( '%s test is disabled via control flag' % self.testName, 'Status INFO (= 20)', 'info' )

    result = self.__checkSharedArea()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    sharedArea = result[ 'Value' ]

    #Check for optional install project URL
    result = self.__checkInstallProjectURL( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    # Change the permissions on the shared area (if a pool account is used)
    if not re.search( '\d$', self.runInfo[ 'identityShort' ] ):
      isPoolAccount = False
    else:
      isPoolAccount = True

    sharedArea = self.__checkWrittableSharedArea( sharedArea )[ 'Value' ]
    
    # Purge shared area if requested.
    result = self.__checkPurgeSharedArea( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    #Install the software now
    result = self.__installSoftware( isPoolAccount, sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )      

    if isPoolAccount:
      result = self.__changePermissions( sharedArea )
      if not result[ 'OK' ]:
        return self.finalize( 'Failed To Change Shared Area Permissions', result['Message'], 'error' )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  ##############################################################################
  # Private methods

  def __checkSAMResults( self ):
    '''
       Checks SAMResults
    '''

    if not 'SAMResults' in self.workflow_commons:
      
      result = S_ERROR( 'No SAMResults key in workflow commons' )
      result[ 'Description' ] = 'Problem determining CE-lhcb-lock test result'
      result[ 'SamResult' ]   = 'error' 
      
      return result

    result = S_OK()

    if int( self.workflow_commons[ 'SAMResults' ][ 'CE-lhcb-lock' ] ) > int( self.samStatus[ 'ok' ] ):
      self.writeToLog( 'Another SAM job is running at this site, disabling software installation test for this CE job' )
      
      result = S_ERROR( 'Status INFO (= 20)' )
      result[ 'Description' ] = '%s test will be disabled' % self.testName
      result[ 'SamResult' ]   = 'info'
      
    return result
  
  def __checkSharedArea( self ):
    '''
       Checks SharedArea
    '''
    
    if not createSharedArea():
      self.log.info( 'Can not get access to Shared Area for SW installation' )
      result = S_ERROR( 'Status ERROR (=50)' )
      result[ 'Description' ] = 'Could not determine shared area for site'
      result[ 'SamResult' ]   = 'error'
      #return self.finalize( 'Could not determine shared area for site', , 'error' )
    
      return result
     
    sharedArea = getSharedArea()
    if not sharedArea or not os.path.exists( sharedArea ):
      # After previous check this error should never occur
      self.log.info( 'Could not determine sharedArea for site %s:\n%s' % ( DIRAC.siteName(), sharedArea ) )
      
      result = S_ERROR( sharedArea )
      result[ 'Description' ] = 'Could not determine shared area for site'
      result[ 'SamResult' ]   = 'critical' 
    
      return result
    
    self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )
    return S_OK( sharedArea )
    
  def __checkWrittableSharedArea( self, sharedArea ): 
    '''
       nasty fix but only way to resolve writeable volume at CERN
    '''
         
    if DIRAC.siteName() in [ 'LCG.CERN.ch', 'LCG.CERN5.ch' ]:
      self.log.info( 'Changing shared area path to writeable volume at CERN' )
      if re.search( '.cern.ch', sharedArea ):
        newSharedArea = sharedArea.replace( 'cern.ch', '.cern.ch' )
        self.writeToLog( 'Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s' % ( sharedArea,
                                                                                                       newSharedArea ) )
        sharedArea = newSharedArea
        os.environ[ 'VO_LHCB_SW_DIR' ] = os.environ[ 'VO_LHCB_SW_DIR' ].replace( 'cern.ch', '.cern.ch' )

    if DIRAC.siteName() in [ 'LCG.IN2P3.fr', 'LCG.IN2P3-T2.fr' ]:
      self.log.info( 'Changing shared area path to writeable volume at IN2P3' )
      if re.search( '.in2p3.fr', sharedArea ):
        newSharedArea = sharedArea.replace( 'in2p3.fr', '.in2p3.fr' )
        _msg = 'Changing path to shared area writeable volume at LCG.IN2P3.fr:\n%s => %s'
        self.writeToLog( _msg % ( sharedArea, newSharedArea ) )
        sharedArea = newSharedArea
        os.environ[ 'VO_LHCB_SW_DIR' ] = os.environ[ 'VO_LHCB_SW_DIR' ].replace( 'in2p3.fr', '.in2p3.fr' )
    
    return S_OK( sharedArea ) 
  
  def __checkInstallProjectURL( self, sharedArea ):
    '''
       Check InstallProjectURL
    '''
    #Check for optional install project URL
    if not self.installProjectURL:
      return S_OK()
      
    self.writeToLog( 'Found specified install_project URL %s' % ( self.installProjectURL ) )
    installProjectName = 'install_project.py'
    
    if os.path.exists( '%s/%s' % ( os.getcwd(), installProjectName ) ):
      self.writeToLog( 'Removing previous install project script from local area' )
      os.remove( '%s/%s' % ( os.getcwd(), installProjectName ) )
    
    installProjectFile = os.path.basename( self.installProjectURL )
    
    if not os.path.exists( '%s/%s' % ( os.getcwd(), installProjectFile ) ):
      
      result = S_ERROR( installProjectFile )
      result[ 'Description' ] = '%s could not be downloaded to local area' % ( self.installProjectURL )
      result[ 'SamResult' ]   = 'error'

      return result      

    else:
      self.writeToLog( 'install_project downloaded from %s to local area' % ( self.installProjectURL ) )
    
    self.writeToLog( 'Copying downloaded install_project to sharedArea %s' % sharedArea )
    
    if not installProjectFile == installProjectName:
      shutil.copy( '%s/%s' % ( os.getcwd(), installProjectFile ), '%s/%s' % ( os.getcwd(), installProjectName ) )
    shutil.copy( '%s/%s' % ( os.getcwd(), installProjectName ), '%s/%s' % ( sharedArea, installProjectName ) )

    return S_OK()
  
  def __checkPurgeSharedArea( self, sharedArea ):
    '''
       Check purgeSharedArea
    '''
        
    # Purge shared area if requested.
    if not self.purgeSharedArea:
      return S_OK()
      
    self.log.info( 'Flag to purge the site shared area at %s is enabled' % sharedArea )
    if self.enable:
      self.log.verbose( 'Enable flag is True, starting shared area deletion' )
      result = self.__deleteSharedArea( sharedArea )
      
      if not result[ 'OK' ]:
        
        result[ 'Description' ] = 'Could not delete software in shared area'
        result[ 'SamResult' ]   = 'critical'  
        return result
        
    else:
      self.log.info( 'Enable flag is False so shared area will not be cleaned' )
    
    return S_OK()
  
  def __checkVersion( self, candidatePackage, isPoolAccount, sharedArea ):
    '''
       Check package version
    '''
  
    appNameVersion = candidatePackage.split( '.' )
    if not len( appNameVersion ) == 2:
      if isPoolAccount:
        self.__changePermissions( sharedArea )
      
      result = S_ERROR( candidatePackage )
      result[ 'Description' ] = 'Could not determine name and version of package:'
      result[ 'SamResult' ]   = 'error'
      return result
    
    return S_OK( appNameVersion )
  
  @staticmethod
  def __getSofwtare():
    '''
       Get lists of active and deprecared software
    '''

    activeSoftware = '/Operations/SoftwareDistribution/Active'
    installList = gConfig.getValue( activeSoftware, [] )
    if not installList:
      result = S_ERROR( activeSoftware )
      result[ 'Description' ] = 'The active list of software could not be retreived from'
      result[ 'SamResult' ]   = 'error'
      return result
      
    deprecatedSoftware = '/Operations/SoftwareDistribution/Deprecated'
    removeList = gConfig.getValue( deprecatedSoftware, [] )
    
    return S_OK( ( installList, removeList ) )
  
  @staticmethod
  def __getLocalPlatforms():
    '''
       Gets local platforms
    '''

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
      
    return S_OK( localPlatforms )
      
  def __installSoftware( self, isPoolAccount, sharedArea ):
    '''
       Installs and removes software
    '''
    
    if not self.enable:
      self.log.info( 'Software installation is disabled via enable flag' )
      return S_OK()

    result = self.__getSofwtare()
    if not result[ 'OK' ]:
      return result
    installList, removeList = result[ 'Value' ]
    
    result = self.__getLocalPlatforms()
    if not result[ 'OK' ]:
      return result
    localPlatforms = result[ 'Value' ]

    for systemConfig in localPlatforms:
      
      _msg = 'The following software packages will be installed:\n%s\nfor system configuration %s'
      self.log.info( _msg % ( '\n'.join( installList ), systemConfig ) )
      
      packageList = gConfig.getValue( '/Operations/SoftwareDistribution/%s' % ( systemConfig ), [] )

      for installPackage in installList:
          
        if not installPackage in packageList:
          _msg = '%s is not supported for system configuration %s, nothing to install.'
          self.log.info( _msg % ( installPackage, systemConfig ) )
          continue
          
        result = self.__installPackage( installPackage, isPoolAccount, sharedArea, 
                                        systemConfig )
        if not result[ 'OK' ]:
          return result

      for removePackage in removeList:
          
        result = self.__removePackage( removePackage, isPoolAccount, sharedArea, systemConfig )
        if not result[ 'OK' ]:
          return result    
    
    return S_OK()
    
  def __installPackage( self, installPackage, isPoolAccount, sharedArea, systemConfig ):
    '''
       Install a package
    '''
    
    appNameVersion = self.__checkVersion( installPackage, isPoolAccount, sharedArea )
    if not appNameVersion[ 'OK' ]:
      return appNameVersion
    appNameVersion = appNameVersion[ 'Value' ]
    
    #Must check that package to install is supported by LHCb for requested system configuration
      
    _msg = 'Attempting to install %s %s for system configuration %s'  
    self.log.info( _msg % ( appNameVersion[ 0 ], appNameVersion[ 1 ], systemConfig ) )
    
    sys.stdout.flush()
    
    orig       = sys.stdout
    catch      = open( self.logFile, 'a' )
    sys.stdout = catch
    result     = False
    
    try:
      result = installApplication( appNameVersion, systemConfig, sharedArea )
    except Exception, x:
      _msg = 'installApplication("%s","%s","%s") failed with exception:\n%s'
      self.log.error( _msg % ( appNameVersion, systemConfig, sharedArea, x ) )
      
    sys.stdout = orig
    catch.close()
    sys.stdout.flush()
    
    if not result: 
      if isPoolAccount:
        self.__changePermissions( sharedArea )
      
      result = S_ERROR( result )
      result[ 'Description' ] = 'Problem during software installation, stopping.'
      result[ 'SamResult' ]   = 'error' 
        
      return result
    
    self.log.info( 'Installation of %s %s for %s successful' % ( appNameVersion[0], appNameVersion[1],
                                                                 systemConfig ) )   
    return S_OK()
  
  def __removePackage( self, removePackage, isPoolAccount, sharedArea, systemConfig ):
    '''
       Remove a package
    '''
    
    appNameVersion = self.__checkVersion( removePackage, isPoolAccount, sharedArea )
    if not appNameVersion[ 'OK' ]:
      return appNameVersion
    appNameVersion = appNameVersion[ 'Value' ]
    
    _msg = 'Attempting to remove %s %s for system configuration %s'
    self.log.info( _msg % ( appNameVersion[ 0 ], appNameVersion[ 1 ], systemConfig ) )
    
    sys.stdout.flush()
    orig       = sys.stdout
    catch      = open( self.logFile, 'a' )
    sys.stdout = catch
    result     = False
    
    try:
      result = removeApplication( appNameVersion, systemConfig, sharedArea )
    except Exception, x:
      _msg = 'removeApplication("%s","%s","%s") failed with exception:\n%s'
      self.log.error( _msg % ( appNameVersion, systemConfig, sharedArea, x ) )
      
    sys.stdout = orig
    catch.close()
    sys.stdout.flush()
    
    #FIXME: #Not sure why it is ignored if this fails - to be reviewed...
    #result = True 
    if not result: # or not result['OK']:
      if isPoolAccount:
        self.__changePermissions( sharedArea )
      
      result = S_ERROR( result )
      result[ 'Description' ] = 'Problem during software installation, stopping.'
      result[ 'SamResult' ]   = 'error' 
        
      return result
    else:
      self.log.info( 'Removal of %s %s for %s successful' % ( appNameVersion[ 0 ], appNameVersion[ 1 ],
                                                              systemConfig ) )
    return S_OK()
    
  def __changePermissions( self, sharedArea ):
    '''
       Change permissions for pool SGM account case in python.
    '''
    
    self.log.verbose( 'Changing permissions to 0775 in shared area %s' % sharedArea )
    self.writeToLog( 'Changing permissions to 0775 in shared area %s' % sharedArea )

    userID = self.runInfo[ 'identityShort' ]

    try:
      
      for dirName, _subDirs, files in os.walk( sharedArea ):
    
        self.log.debug( 'Changing file permissions in directory %s' % dirName )
        if os.path.isdir( dirName ) and not os.path.islink( dirName ) and os.stat( dirName )[4] == userID:
          try:
            os.chmod( dirName, 0775 )
          except Exception, x:
            self.log.error( 'Can not change permission to dir:', dirName )
            self.log.error( 'Is dir:  ', os.path.isfile( dirName ) )
            self.log.error( 'Is link: ', os.path.islink( dirName ) )
            self.log.error( 'Is exits:', os.path.exists( dirName ) )
            raise x

        for toChange in files:
          filename = os.path.join( dirName, toChange )
          
          if os.path.isfile( filename ) and not os.path.islink( filename ) and os.stat( filename )[4] == userID :
            
            try:
              os.chmod( filename, 0775 )
            except Exception, x:
              self.log.error( 'Can not change permission to file:', filename )
              self.log.error( 'Is file: ', os.path.isfile( filename ) )
              self.log.error( 'Is link: ', os.path.islink( filename ) )
              self.log.error( 'Is exits:', os.path.exists( filename ) )
              raise x
            
    except OSError, x:
      self.log.error( 'Problem changing shared area permissions', str( x ) )
      return S_ERROR( x )

    self.log.info( 'Permissions in shared area %s updated successfully' % ( sharedArea ) )
    self.writeToLog( 'Permissions in shared area %s updated successfully' % ( sharedArea ) )
    return S_OK()

  def __deleteSharedArea( self, sharedArea ):
    """Remove all files in shared area.
    """
    self.log.verbose( 'Removing all files in shared area %s' % sharedArea )
    self.writeToLog( 'Removing all files in shared area %s' % sharedArea )
    try:
      for fdir in os.listdir( sharedArea ):
        if os.path.isfile( '%s/%s' % ( sharedArea, fdir ) ):
          os.remove( '%s/%s' % ( sharedArea, fdir ) )
        elif os.path.isdir( '%s/%s' % ( sharedArea, fdir ) ):
          self.log.verbose( 'Removing directory %s/%s' % ( sharedArea, fdir ) )
          self.writeToLog( 'Removing directory %s/%s' % ( sharedArea, fdir ) )
          shutil.rmtree( '%s/%s' % ( sharedArea, fdir ) )
    except OSError, x:
      self.log.error( 'Problem deleting shared area ', str( x ) )
      return S_ERROR( x )

    self.log.info( 'Shared area %s successfully purged' % ( sharedArea ) )
    self.writeToLog( 'Shared area %s successfully purged' % ( sharedArea ) )
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF