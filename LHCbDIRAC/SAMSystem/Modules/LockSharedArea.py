''' LHCb LockSharedArea SAM Test Module
'''

import os
import re
import time
import DIRAC

from DIRAC                                               import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSharedArea, createSharedArea
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM              import ModuleBaseSAM

__RCSID__ = "$Id$"

class LockSharedArea( ModuleBaseSAM ):

  def __init__( self ):
    '''
        Standard constructor for SAM Module
    '''
    ModuleBaseSAM.__init__( self )

    self.logFile  = 'sam-lock.log'
    self.testName = 'CE-lhcb-lock'
    self.lockFile = 'DIRAC-SAM-Test-Lock'

    # Validity of the lock
    self.lockValidity     = Operations().getValue( 'SAM/LockValidity', 24 * 60 * 60 )
    #Workflow parameters for the test
    self.forceLockRemoval = False
    #Global parameter affecting behaviour
    self.safeMode         = False
  
  def resolveInputVariables( self ):
    '''
        By convention the workflow parameters are resolved here.
    '''

    ModuleBaseSAM.resolveInputVariables( self )

    if 'forceLockRemoval' in self.step_commons:
      self.forceLockRemoval = self.step_commons[ 'forceLockRemoval' ]
      if not isinstance( self.forceLockRemoval, bool ):
        self.log.warn( 'Force lock flag set to non-boolean value %s, setting to False' % self.forceLockRemoval )
        self.forceLockRemoval = False

    if 'SoftwareInstallationTest' in self.workflow_commons:
      safeFlag = self.workflow_commons[ 'SoftwareInstallationTest' ]
      if safeFlag == 'False':
        self.safeMode = True

    self.log.verbose( 'Force lock flag is set to %s' % self.forceLockRemoval )
    return S_OK()

  def _execute( self ):
    '''
       The main execution method of the LockSharedArea module.
    '''

    isPoolAccount = self.__checkAccounts()[ 'Value' ]

    #If running in safe mode stop here and return S_OK()
    result = self.__checkSafeMode()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    result = self.__checkUmask( isPoolAccount )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    result = self.__checkSharedArea()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    sharedArea = result[ 'Value' ]

    result = self.__checkSharedAreaContents( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    result = self.__checkSharedAreaLink( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    result = self.__checkForceLockRemoval( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    result = self.__checkSAMLockFile( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    result = self.__checkLockFile( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    result = self.__checkInstallProject( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    self.setJobParameter( 'NewSAMLock', 'Created on %s' % ( time.asctime() ) )
    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )

    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  ##############################################################################
  # Protected methods

  def __checkAccounts( self ):
    '''
       Checks Accounts
    '''
    
    self.log.info( '>> __checkAccounts' )
    self.log.info( 'Current account: %s' % self.runInfo[ 'identity' ] )
    
    # a username is a POOL account if it finishes with digit
    if not re.search( '\d$', self.runInfo[ 'identityShort' ] ):
      self.log.info( '%s uses static accounts' % DIRAC.siteName() )
      isPoolAccount = False
    else:
      self.log.info( '%s uses pool accounts' % DIRAC.siteName() )
      isPoolAccount = True
      
    return S_OK( isPoolAccount )  

  def __checkSafeMode( self ):
    '''
       Checks safe mode, exits if it is on
    '''
    
    self.log.info( '>> __checkSafeMode' )
    
    result = S_OK()
    
    #If running in safe mode stop here and return S_OK()
    if self.safeMode:
      self.log.info( 'We are running in SAM safe mode so no lock file will be created' )
      self.setApplicationStatus( '%s Successful (Safe Mode)' % self.testName )
      
      result = S_ERROR( 'Status OK (= 10)' )
      result[ 'Description' ] = '%s Test Successful (Safe Mode)' % self.testName
      result[ 'SamResult' ]   = 'ok'
        
    return result

  def __checkUmask( self, isPoolAccount ):
    '''
       Checks umask
    '''
    
    self.log.info( '>> __checkUmask' )
    
    result = self.runCommand( 'Checking current umask', 'umask' )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'umask returned non-zero status'
      result[ 'SamResult' ]   = 'error'
      return result

    self.log.info( 'Current umask: %s' % result[ 'Value' ] )
    
    if isPoolAccount:
      self.log.info( 'Is poolAccount' )
      if not result[ 'Value' ].count( '0002' ):
        self.log.info( 'Changing current umask to 0002' )
        try:
          os.umask( 0002 )
        except OSError, x:
          result = S_ERROR( x )
          result[ 'Description' ] = 'excepton during umask'
          result[ 'SamResult' ]   = 'error'
          return result
    else:
      self.log.info( 'Is NOT poolAccount' )
      if not result[ 'Value' ].count( '0022' ):
        self.log.info( 'Changing current umask to 0022' )
        try:
          os.umask( 0022 )
        except OSError, x:
          result = S_ERROR( x )
          result[ 'Description' ] = 'excepton during umask'
          result[ 'SamResult' ]   = 'error'
          return result
    
    return S_OK()    

  def __checkSharedArea( self ):
    '''
       Checks sharedArea
    '''
    
    self.log.info( '>> __checkSharedArea' )
    
    sharedArea = getSharedArea()
    
    if not sharedArea:
      self.log.info( 'Could not determine sharedArea for site %s' % DIRAC.siteName() ) 
      self.log.info( '%s trying to create it' % sharedArea )
      createsharedArea = createSharedArea()
      if not createsharedArea:
        result = S_ERROR( sharedArea )
        result[ 'Description' ] = 'Could not create sharedArea for site %s:' % DIRAC.siteName()
        result[ 'SamResult' ]   = 'error'
        return result
      sharedArea = getSharedArea()

    else:
      self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )
    # Check if the shared area is cernvmfs one.
    # if yes then return Error
      if os.path.exists( os.path.join( sharedArea, 'etc', 'cernvmfs' ) ):
        self.log.info( 'Software shared area for site %s is using CERNVMFS' % ( DIRAC.siteName() ) )
        result = S_ERROR( 'Read-Only volume' )
        result[ 'Description' ] = 'Could not install (CERNVMFS) for site %s:' % DIRAC.siteName()
        result[ 'SamResult' ]   = 'warning'
        return result

    #nasty fix but only way to resolve writeable volume at CERN
    if DIRAC.siteName() == 'LCG.CERN.ch' or DIRAC.siteName() == 'LCG.CERN5.ch':
      self.log.info( 'Changing shared area path to writeable volume at CERN' )
      if re.search( '.cern.ch', sharedArea ):
        newSharedArea = sharedArea.replace( 'cern.ch', '.cern.ch' )
        _msg = 'Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s'
        self.writeToLog( _msg % ( sharedArea, newSharedArea ) )
        sharedArea = newSharedArea

    return S_OK( sharedArea )

  def __checkSharedAreaContents( self, sharedArea ):
    '''
       Checks sharedAreaContents
    '''
    
    self.log.info( '>> __checkSharedAreaContents' )
    
    self.log.info( 'Checking shared area contents: %s' % ( sharedArea ) )
    result = self.runCommand( 'Checking contents of shared area directory: %s' % sharedArea, 'ls -al %s' % sharedArea )
    if not result['OK']:
      result[ 'Description' ] = 'Could not list contents of shared area'
      result[ 'SamResult' ]   = 'error'
    
    return result   
 
  def __checkSharedAreaLink( self, sharedArea ): 
    '''
       Checks sharedAreaLink
    '''

    self.log.info( '>> __checkSharedAreaLink' )

    self.log.verbose( 'Trying to resolve shared area link problem' )
    
    if os.path.exists( '%s/lib' % sharedArea ):
      if os.path.islink( '%s/lib' % sharedArea ):
    
        self.log.info( 'Removing link %s/lib' % sharedArea )
        result = self.runCommand( 'Removing link in shared area', 'rm -fv %s/lib' % sharedArea, check = True )
        if not result[ 'OK' ]:
          result[ 'Description' ] = 'Could not remove link in shared area'
          result[ 'SamResult' ]   = 'error' 
          return result
                
      else:
        self.log.info( '%s/lib is not a link so will not be removed' % sharedArea )
    else:
      self.log.info( 'Link in shared area %s/lib does not exist' % sharedArea )
 
    return S_OK()

  def __checkForceLockRemoval( self, sharedArea ):
    '''
       Checks forceLockRemoval
    '''
    
    self.log.info( '>> __checkForceLockRemoval' )
    
    if self.forceLockRemoval:

      self.log.info( 'Deliberately removing SAM lock file' )
      cmd = 'rm -fv %s/%s' % ( sharedArea, self.lockFile )
      
      result = self.runCommand( 'Flag enabled to forcefully remove current %s' % self.lockFile, cmd, check = True )
      
      if not result[ 'OK' ]:
        self.setApplicationStatus( 'Could Not Remove Lock File' )
        self.log.warn( result[ 'Message' ] )
        
        result[ 'Message' ]     = self.lockFile
        result[ 'Description' ] = 'Could not remove existing lock file via flag' 
        result[ 'SamResult' ]   = 'critical'
        
        return result

      self.setJobParameter( 'ExistingSAMLock', 'Deleted via SAM test flag on %s' % time.asctime() )
      
    return S_OK()  

  def __checkSAMLockFile( self, sharedArea ):
    '''
       Checks SAMLockFile
    '''
    
    self.log.info( '>> __checkSAMLockFile' )
    
    self.log.info( 'Checking SAM lock file: %s' % self.lockFile )
    
    if os.path.exists( '%s/%s' % ( sharedArea, self.lockFile ) ):
      self.log.info( 'Another SAM job has established a lock on the shared area at %s' % sharedArea )
      curtime = time.time()
      fileTime = os.stat( '%s/%s' % ( sharedArea, self.lockFile ) )[ 8 ]
      
      if curtime - fileTime > self.lockValidity:
      
        self.log.info( 'SAM lock file present for > %s secs, deleting' % self.lockValidity )
        cmd = 'rm -fv %s/%s' % ( sharedArea, self.lockFile )
        _msg = 'Current lock file %s present for longer than %s seconds'
        
        result = self.runCommand(  _msg % ( self.lockFile, self.lockValidity ), cmd, check = True )
        self.setApplicationStatus( 'Could Not Remove Old Lock File' )
        
        if not result[ 'OK' ]:
          self.log.warn( result[ 'Message' ] )
          
          result[ 'Description' ] = 'Could not remove existing lock file exceeding maximum validity' 
          result[ 'SamResult' ]   = 'critical'
          return result
        
        self.setJobParameter( 'ExistingSAMLock', 'Removed on %s after exceeding maximum validity' % ( time.asctime() ) )
      
      else:
        #unique to this test, prevent execution of software installation via 'notice' status
        _msg = 'Another SAM job has been running at this site for less than %s'
        _msg += ' seconds disabling software installation test'
        
        self.log.info( _msg % self.lockValidity )
        self.writeToLog( _msg % self.lockValidity )
        
        self.setApplicationStatus( 'Shared Area Lock Exists' )
                        
        result = S_ERROR( 'Status NOTICE (= 30)' )
        result[ 'Description' ] = '%s test running at same time as another SAM job' % self.testName
        result[ 'SamResult' ]   = 'notice'
        return result

    return S_OK()

  def __checkLockFile( self, sharedArea ):
    '''
       Checks Lock file
    '''
    
    self.log.info( '>> __checkLockFile' )
    
    cmd = 'touch %s/%s' % ( sharedArea, self.lockFile )
    result = self.runCommand( 'Creating SAM lock file', cmd, check = True )
    
    if not result[ 'OK' ]:
      
      self.log.warn( result[ 'Message' ] )
      self.log.info( 'Trying to change permissions: %s' % ( sharedArea ) )
      
      try:
        os.chmod( sharedArea, 0775 )
      except OSError:
        self.setApplicationStatus( 'Could Not Create Lock File' )
        
        result = S_ERROR( sharedArea )
        result[ 'Description' ] = 'Could not change permissions'
        result[ 'SamResult' ]   = 'critical' 
        return result
      
      cmd = 'touch %s/%s' % ( sharedArea, self.lockFile )
      result = self.runCommand( 'Creating SAM lock file', cmd, check = True )
      
      if not result[ 'OK' ]:
        self.setApplicationStatus( 'Could Not Create Lock File' )
        
        result = S_ERROR( '%s/%s' % ( sharedArea, self.lockFile ) )
        result[ 'Description' ] = 'Could not create lock file'
        result[ 'SamResult' ]   = 'critical' 
        return result
  
    return S_OK()  

  def __checkInstallProject( self, sharedArea ):
    '''
       Checks InstallProject
    '''
    
    self.log.info( '>> __checkInstallProject' )
    
    if os.path.exists( '%s/install_project.py' % ( sharedArea ) ):
      
      self.log.info( 'Removing install_project from SharedArea' )
      cmd = 'rm -fv %s/install_project.py' % ( sharedArea )
      result = self.runCommand( 'Removing install_project from SharedArea', cmd, check = True )
      
      if not result[ 'OK' ]:
        self.setApplicationStatus( 'Could Not Remove File' )
        self.log.warn( result[ 'Message' ] )
        
        result[ 'Description' ] = 'Could not remove install_project from SharedArea'
        result[ 'SamResult' ]   = 'critical' 
        return result
    
    return S_OK()
  
#FIXME: unused ?  
#  def __changePermissions( self, sharedArea ):
#    '''
#       Change permissions for pool SGM account case in python.
#    '''
#    
#    self.log.verbose( 'Changing permissions to 0775 in shared area %s' % sharedArea )
#    self.writeToLog( 'Changing permissions to 0775 in shared area %s' % sharedArea )
#
#    userID = self.runInfo[ 'identityShort' ]
#
#    try:
#      
#      for dirName, _subDirs, files in os.walk( sharedArea ):
#        self.log.debug( 'Changing file permissions in directory %s' % dirName )
#        if os.stat( '%s' % ( dirName ) )[4] == userID and not os.path.islink( '%s' % ( dirName ) ):
#          os.chmod( '%s' % ( dirName ), 0775 )
#        
#        for toChange in files:
#          
#          pathIsLink = os.path.islink( '%s/%s' % ( dirName, toChange ) )
#          if os.stat( '%s/%s' % ( dirName, toChange ) )[4] == userID and not pathIsLink:
#            os.chmod( '%s/%s' % ( dirName, toChange ), 0775 )
#            
#    except OSError, x:
#      self.log.error( 'Problem changing shared area permissions', str( x ) )
#      return S_ERROR( x )
#
#    self.log.info( 'Permissions in shared area %s updated successfully' % ( sharedArea ) )
#    self.writeToLog( 'Permissions in shared area %s updated successfully' % ( sharedArea ) )
#    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF