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

from DIRAC import S_OK, S_ERROR, gConfig

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation import removeApplication, installApplication
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM             import ModuleBaseSAM
from LHCbDIRAC.SAMSystem.Utilities                         import Utils

__RCSID__ = "$Id$"

class SoftwareInstallation( ModuleBaseSAM ):
  """ SoftwareInstallation SAM class """

  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )
    
    #self.logFile  = 'sam-install.log'
    #self.testName = 'CE-lhcb-install'

    #Workflow parameters for the test
    self.purgeSharedArea   = False
    self.softwareFlag      = False
    self.installProjectURL = None

  def resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    
    ModuleBaseSAM.resolveInputVariables( self )
    
    if 'purgeSharedAreaFlag' in self.step_commons:
      self.purgeSharedArea = self.step_commons['purgeSharedAreaFlag']
      if not isinstance( self.purgeSharedArea, bool ):
        self.log.warn( 'Purge shared area flag set to non-boolean value %s, setting to False' % self.purgeSharedArea )
        self.purgeSharedArea = False

    if 'softwareFlag' in self.step_commons:
      self.softwareFlag = self.step_commons['softwareFlag']
      if not isinstance( self.softwareFlag, bool ):
        self.log.warn( 'Software flag set to non-boolean value %s, setting to False' % self.softwareFlag )
        self.softwareFlag = False

    if 'installProjectURL' in self.step_commons:
      self.installProjectURL = self.step_commons[ 'installProjectURL' ]
      if not isinstance( self.installProjectURL, str ) or not self.installProjectURL:
        self.log.warn( 'Install project URL not set to non-zero string parameter, setting to None' )
        #Default installProjectURL
        self.installProjectURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/install_project.py'

    self.log.verbose( 'purgeSharedArea = %s' % self.purgeSharedArea )
    self.log.verbose( 'softwareFlag = %s' % self.softwareFlag )  
    self.log.verbose( 'installProjectURL = %s' % self.installProjectURL )
    return S_OK()

  def _execute( self ):
    """The main execution method of the SoftwareInstallation module.
    """
  
    #result = self.__checkLockResult()
    #if not result[ 'OK' ]:
    #  return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    #This only applies if we are installing Software,
    result = Utils.checkCernVMFS( self.sharedArea, self.log )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    #FIXME: check if it is there, if so, DO NOT FUCKING try to install it ! I've said.
    #Check for optional install project URL
    result = self.__checkInstallProjectURL()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )    

    #ORDER MATTERS !!
    if not self.softwareFlag:
      self.log.verbose( 'Software installation is disabled via control flag' )
      return self.finalize( 'Software installation is disabled via control flag', 'Status OK (= 0)', 'ok' )

    # Change the permissions on the shared area (if a pool account is used)
    if not re.search( '\d$', self.runInfo[ 'identityShort' ] ):
      isPoolAccount = False
    else:
      isPoolAccount = True
    
    # Purge shared area if requested.
    result = self.__checkPurgeSharedArea()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    #Install the software now
    result = self.__installSoftware( isPoolAccount )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )      

    if isPoolAccount:
      result = self.__changePermissions()
      if not result[ 'OK' ]:
        return self.finalize( 'Failed To Change Shared Area Permissions', result['Message'], 'error' )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 0)', 'ok' )

  ##############################################################################
  # Private methods

  def __checkLockResult( self ):
    '''
       Checks result of LockSharedArea
    '''
    
    #FIXME:
    # Do we really need this step ?
    # If there was an error previously, we will never run this module.
    
    self.log.info( '>> __checkLockResult' )    
    
    result = S_OK()

    status = self.workflow_commons[ 'SAMResults' ].get( 'CE-lhcb-lock', 2 )

    if status > int( self.nagiosStatus[ 'ok' ] ):
      self.writeToLog( 'The status of the lock phase does not look good %s' % status )
      
      result = S_ERROR( 'Status INFO (= 1)' )
      result[ 'Description' ] = '%s test will be disabled' % self.testName
      result[ 'SamResult' ]   = 'warning'
      
    return result
  
  def __checkInstallProjectURL( self ):
    '''
       Check InstallProjectURL
    '''
    self.log.info( '>> __checkInstallProjectURL' )
    
    #Check for optional install project URL
    if not self.installProjectURL:
      self.log.verbose( 'installProjectURL is empty "%s"' % self.installProjectURL )
      return S_OK()
      
    self.writeToLog( 'Found install_project URL %s' % self.installProjectURL )
    installProjectName = 'install_project.py'
    
    if os.path.exists( '%s/%s' % ( os.getcwd(), installProjectName ) ):
      self.writeToLog( 'Removing previous install project script from local area' )
      os.remove( '%s/%s' % ( os.getcwd(), installProjectName ) )
    
    installProjectFile = os.path.basename( self.installProjectURL )
    _localname, _headers = urllib.urlretrieve( self.installProjectURL, installProjectFile )
    
    if not os.path.exists( '%s/%s' % ( os.getcwd(), installProjectFile ) ):
      
      self.log.error( "%s could not be downloaded to local area" % self.installProjectURL ) 
            
      result = S_ERROR( installProjectFile )
      result[ 'Description' ] = '%s could not be downloaded to local area' % ( self.installProjectURL )
      result[ 'SamResult' ]   = 'error'
      return result      

    else:
      self.log.verbose( "Successfuly downloaded from %s to local area" % self.installProjectURL )
      self.writeToLog( 'install_project downloaded from %s to local area' % self.installProjectURL )
    
    self.writeToLog( 'Copying downloaded install_project to sharedArea %s' % self.sharedArea )
    
    if not installProjectFile == installProjectName:
      src, dst = '%s/%s' % ( os.getcwd(), installProjectFile ), '%s/%s' % ( os.getcwd(), installProjectName )
      self.log.verbose( '%s differs from %s' % ( installProjectFile, installProjectName ) )
      shutil.copy( src, dst )

    src, dst = '%s/%s' % ( os.getcwd(), installProjectName ), '%s/%s' % ( self.sharedArea, installProjectName )
    self.log.verbose( 'Copying downloaded install_project to sharedArea %s' % self.sharedArea )
    self.log.verbose( "src: '%s'" % src )
    self.log.verbose( "dst: '%s'" % dst )
    shutil.copy( src, dst )

    return S_OK()
  
  def __checkPurgeSharedArea( self ):
    '''
       Check purgeSharedArea
    '''
    self.log.info( '>> __checkPurgeSharedArea' )
        
    # Purge shared area if requested.
    if not self.purgeSharedArea:
      self.log.verbose( "purgeSharedArea disabled this check" )
      return S_OK()
      
    self.log.info( 'Flag to purge the site shared area at %s is enabled' % self.sharedArea )

    result = self.__deleteSharedArea()
      
    if not result[ 'OK' ]:
      
      result[ 'Description' ] = 'Could not delete software in shared area'
      result[ 'SamResult' ]   = 'error'  
      return result
            
    return S_OK()
  
  def __checkVersion( self, candidatePackage, isPoolAccount ):
    '''
       Check package version
    '''
    self.log.info( '>> __checkVersion' )
  
    appNameVersion = candidatePackage.split( '.' )
    if not len( appNameVersion ) == 2:
      if isPoolAccount:
        self.__changePermissions()
      
      result = S_ERROR( candidatePackage )
      result[ 'Description' ] = 'Could not determine name and version of package:'
      result[ 'SamResult' ]   = 'error'
      return result
    
    return S_OK( appNameVersion )
  
  def __getSofwtare( self ):
    '''
       Get lists of active and deprecared software
    '''
    self.log.verbose( '>> __getSoftware' )

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
      
  def __installSoftware( self, isPoolAccount ):
    '''
       Installs and removes software
    '''
    self.log.info( '>> __installSoftware' )
    
    result = self.__getSofwtare()
    if not result[ 'OK' ]:
      return result
    installList, removeList = result[ 'Value' ]
    
    result = Utils.getLocalPlatforms()
    if not result[ 'OK' ]:
      return result
    localPlatforms = result[ 'Value' ]

    for systemConfig in localPlatforms:
      
      _msg = 'The following software packages will be installed:\n%s\nfor system configuration %s'
      self.log.info( _msg % ( '\n'.join( installList ), systemConfig ) )
      
      packageList = gConfig.getValue( '/Operations/SoftwareDistribution/%s' % ( systemConfig ), [] )

      for installPackage in installList:
          
        if not installPackage in packageList:
          _msg = '%s is not supported for system configuration %s, skipping.'
          self.log.verbose( _msg % ( installPackage, systemConfig ) )
          continue
          
        result = self.__installPackage( installPackage, isPoolAccount, systemConfig )
        if not result[ 'OK' ]:
          return result

      for removePackage in removeList:
          
        result = self.__removePackage( removePackage, isPoolAccount, systemConfig )
        if not result[ 'OK' ]:
          return result    
    
    return S_OK()
    
  def __installPackage( self, installPackage, isPoolAccount, systemConfig ):
    '''
       Install a package
    '''
    self.log.verbose( '>> __installPackage' )  
    
    appNameVersion = self.__checkVersion( installPackage, isPoolAccount )
    if not appNameVersion[ 'OK' ]:
      return appNameVersion
    appNameVersion = appNameVersion[ 'Value' ]
    
    #Must check that package to install is supported by LHCb for requested system configuration
      
    _msg = 'Installing %s %s for system configuration %s'  
    self.log.info( _msg % ( appNameVersion[ 0 ], appNameVersion[ 1 ], systemConfig ) )
    
    # This trick is ugly as hell...
    sys.stdout.flush()
    
    orig       = sys.stdout
    catch      = open( self.logFile, 'a' )
    sys.stdout = catch
    result     = False
    
    try:
      result = installApplication( appNameVersion, systemConfig, self.sharedArea )
    except Exception, x:
      _msg = 'installApplication("%s","%s","%s") failed with exception:\n%s'
      self.log.error( _msg % ( appNameVersion, systemConfig, self.sharedArea, x ) )
      
    sys.stdout = orig
    catch.close()
    sys.stdout.flush()
    
    #FIXME: why ???
    if not result:
      if isPoolAccount:
        self.__changePermissions()
      
      result = S_ERROR( result )
      result[ 'Description' ] = 'Problem during software installation, stopping.'
      result[ 'SamResult' ]   = 'error' 
        
      return result
    
    self.log.info( 'Installation of %s %s for %s successful' % ( appNameVersion[0], appNameVersion[1],
                                                                 systemConfig ) )   
    return S_OK()
  
  def __removePackage( self, removePackage, isPoolAccount, systemConfig ):
    '''
       Remove a package
    '''
    self.log.info( '>> __removePackage' )
    
    appNameVersion = self.__checkVersion( removePackage, isPoolAccount )
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
      result = removeApplication( appNameVersion, systemConfig, self.sharedArea )
    except Exception, x:
      _msg = 'removeApplication("%s","%s","%s") failed with exception:\n%s'
      self.log.error( _msg % ( appNameVersion, systemConfig, self.sharedArea, x ) )
      
    sys.stdout = orig
    catch.close()
    sys.stdout.flush()
    
    #FIXME: #Not sure why it is ignored if this fails - to be reviewed...
    #result = True 
    if not result: # or not result['OK']:
      if isPoolAccount:
        self.__changePermissions()
      
      result = S_ERROR( result )
      result[ 'Description' ] = 'Problem during software installation, stopping.'
      result[ 'SamResult' ]   = 'error' 
        
      return result
    else:
      self.log.info( 'Removal of %s %s for %s successful' % ( appNameVersion[ 0 ], appNameVersion[ 1 ],
                                                              systemConfig ) )
    return S_OK()
    
  def __changePermissions( self ):
    '''
       Change permissions for pool SGM account case in python.
    '''
    self.log.info( '>> __changePermissions' )    
    
    self.log.verbose( 'Changing permissions to 0775 in shared area %s' % self.sharedArea )
    self.writeToLog( 'Changing permissions to 0775 in shared area %s' % self.sharedArea )

    userID = self.runInfo[ 'identityShort' ]

    try:
      
      for dirName, _subDirs, files in os.walk( self.sharedArea ):
    
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

    self.log.info( 'Permissions in shared area %s updated successfully' % ( self.sharedArea ) )
    self.writeToLog( 'Permissions in shared area %s updated successfully' % ( self.sharedArea ) )
    return S_OK()

  def __deleteSharedArea( self ):
    """Remove all files in shared area.
    """
    self.log.info( '>> __deleteSharedArea' )
    
    self.log.verbose( 'Removing all files in shared area %s' % self.sharedArea )
    self.writeToLog( 'Removing all files in shared area %s' % self.sharedArea )
    try:
      for fdir in os.listdir( self.sharedArea ):
        if os.path.isfile( '%s/%s' % ( self.sharedArea, fdir ) ):
          os.remove( '%s/%s' % ( self.sharedArea, fdir ) )
        elif os.path.isdir( '%s/%s' % ( self.sharedArea, fdir ) ):
          self.log.verbose( 'Removing directory %s/%s' % ( self.sharedArea, fdir ) )
          self.writeToLog( 'Removing directory %s/%s' % ( self.sharedArea, fdir ) )
          shutil.rmtree( '%s/%s' % ( self.sharedArea, fdir ) )
    except OSError, x:
      self.log.error( 'Problem deleting shared area ', str( x ) )
      return S_ERROR( x )

    self.log.info( 'Shared area %s successfully purged' % self.sharedArea )
    self.writeToLog( 'Shared area %s successfully purged' % self.sharedArea ) 
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF