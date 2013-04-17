''' LHCb LockSharedArea SAM Test Module
'''

#import os
#import re
#import time
#import DIRAC
#
#from DIRAC                                               import S_OK, S_ERROR
#from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
#
#from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM              import ModuleBaseSAM
#
#__RCSID__ = "$Id$"
#
#class LockSharedArea( ModuleBaseSAM ):
#
#  def __init__( self ):
#    '''
#        Standard constructor for SAM Module
#    '''
#    ModuleBaseSAM.__init__( self )
#
#    #self.logFile  = 'sam-lock.log'
#    #self.testName = 'CE-lhcb-lock'
#    self.lockFile = 'DIRAC-SAM-Test-Lock'
#
#    # Validity of the lock
#    self.lockValidity     = Operations().getValue( 'SAM/LockValidity', 24 * 60 * 60 )
#    #Workflow parameters for the test
#    self.forceLockRemoval = False
#    #Global parameter affecting behaviour
##    self.safeMode         = False
#  
#  def resolveInputVariables( self ):
#    '''
#        By convention the workflow parameters are resolved here.
#    '''
#
#    ModuleBaseSAM.resolveInputVariables( self )
#
#    if 'forceLockRemoval' in self.step_commons:
#      self.forceLockRemoval = self.step_commons[ 'forceLockRemoval' ]
#      if not isinstance( self.forceLockRemoval, bool ):
#        self.log.warn( 'Force lock flag set to non-boolean value %s, setting to False' % self.forceLockRemoval )
#        self.forceLockRemoval = False
#
##    #FIXME: this is a parameter of SoftwareInstallationModule !!
##    if 'SoftwareInstallationTest' in self.workflow_commons:
##      safeFlag = self.workflow_commons[ 'SoftwareInstallationTest' ]
##      if safeFlag == 'False':
##        self.safeMode = True
#
#    self.log.verbose( 'forceLockRemoval = %s' % self.forceLockRemoval )
##    self.log.verbose( 'safeMode = %s' % self.safeMode )
#    
#    return S_OK()
#
#  def _execute( self ):
#    '''
#       The main execution method of the LockSharedArea module.
#    '''
#
#    isPoolAccount = self.__checkAccounts()[ 'Value' ]
#
#    #If running in safe mode stop here and return S_OK()
##    result = self.__checkSafeMode()
##    if not result[ 'OK' ]:
##      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    result = self.__checkUmask( isPoolAccount )
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    result = self.__checkSharedAreaContents()
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    result = self.__checkSharedAreaLink()
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    result = self.__checkForceLockRemoval()
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    result = self.__checkSAMLockFile()
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    result = self.__checkSetLockFile()
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    result = self.__checkInstallProject()
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
#
#    self.setJobParameter( 'NewSAMLock', 'Created on %s' % ( time.asctime() ) )
#    self.log.info( '<>' )
#    self.log.info( 'Test %s completed successfully' % self.testName )
#    self.setApplicationStatus( '%s Successful' % self.testName )
#
#    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )
#
#  ##############################################################################
#  # Protected methods
#
#  def __checkAccounts( self ):
#    '''
#       Checks Accounts
#    '''
#    
#    self.log.info( '>> __checkAccounts' )
#    self.log.info( 'Current account: %s' % self.runInfo[ 'identity' ] )
#    
#    # a username is a POOL account if it finishes with digit
#    if not re.search( '\d$', self.runInfo[ 'identityShort' ] ):
#      self.log.info( '%s uses static accounts' % DIRAC.siteName() )
#      isPoolAccount = False
#    else:
#      self.log.info( '%s uses pool accounts' % DIRAC.siteName() )
#      isPoolAccount = True
#      
#    return S_OK( isPoolAccount )  
#
##  def __checkSafeMode( self ):
##    '''
##       Checks safe mode, exits if it is on
##    '''
##    
##    self.log.info( '>> __checkSafeMode' )
##    
##    result = S_OK()
##    
##    #If running in safe mode stop here and return S_ERROR() -> to run the finalize
##    if self.safeMode:
##      self.log.info( 'We are running in SAM safe mode so no lock file will be created' )
##      self.setApplicationStatus( '%s Successful (Safe Mode)' % self.testName )
##      
##      result = S_ERROR( 'Status OK (= 10)' )
##      result[ 'Description' ] = '%s Test Successful (Safe Mode)' % self.testName
##      result[ 'SamResult' ]   = 'ok'
##        
##    return result
#
#  def __checkUmask( self, isPoolAccount ):
#    '''
#       Checks umask
#    '''
#    
#    self.log.info( '>> __checkUmask' )
#    
#    result = self.runCommand( 'Checking current umask', 'umask' )
#    if not result[ 'OK' ]:
#      result[ 'Description' ] = 'umask returned non-zero status'
#      result[ 'SamResult' ]   = 'error'
#      return result
#
#    self.log.info( 'Current umask: %s' % result[ 'Value' ] )
#    
#    if isPoolAccount:
#      self.log.info( 'Is poolAccount' )
#      mask = 0002
#    else:
#      self.log.info( 'Is NOT poolAccount' )
#      mask = 0022
#        
#    if not result[ 'Value' ].count( '0002' ):
#      self.log.info( 'Changing current umask to %s' % mask )
#      try:
#        os.umask( mask )
#      except OSError, x:
#        result = S_ERROR( x )
#        result[ 'Description' ] = 'excepton during umask'
#        result[ 'SamResult' ]   = 'error'
#        return result
#    
#    return S_OK()
#  
#  def __checkSharedAreaContents( self ):
#    '''
#       Checks sharedAreaContents
#    '''
#    
#    self.log.info( '>> __checkSharedAreaContents' )
#    
#    self.log.info( 'Checking shared area contents: %s' % ( self.sharedArea ) )
#    result = self.runCommand( 'Checking contents of shared area directory: %s' % self.sharedArea, 
#                              'ls -al %s' % self.sharedArea )
#    if not result['OK']:
#      result[ 'Description' ] = 'Could not list contents of shared area'
#      result[ 'SamResult' ]   = 'error'
#    
#    return result   
# 
#  def __checkSharedAreaLink( self ): 
#    '''
#       Checks sharedAreaLink
#    '''
#
#    self.log.info( '>> __checkSharedAreaLink' )
#
#    self.log.verbose( 'Trying to resolve shared area link problem' )
#    
#    libPath = '%s/lib' % self.sharedArea
#    
#    if os.path.exists( libPath ):
#      self.log.info( 'Link in shared area %s does not exist, nothing to do.' % libPath )
#    else:
#      
#      if os.path.islink( libPath ):
#    
#        self.log.info( 'Removing link %s' % libPath )
#        result = self.runCommand( 'Removing link in shared area', 'rm -fv %s' % libPath, check = True )
#        if not result[ 'OK' ]:
#          result[ 'Description' ] = 'Could not remove link in shared area'
#          result[ 'SamResult' ]   = 'error' 
#          return result
#                
#      else:
#        self.log.info( '%s is not a link so will not be removed' % libPath )
# 
#    return S_OK()
#
#  def __checkForceLockRemoval( self ):
#    '''
#       Checks forceLockRemoval
#    '''
#    
#    self.log.info( '>> __checkForceLockRemoval' )
#    
#    if not self.forceLockRemoval:
#      self.log.verbose( 'Force removal flag is false, nothing to do.' )
#
#    else:
#      
#      self.log.info( 'Deliberately removing SAM lock file' )
#      cmd = 'rm -fv %s/%s' % ( self.sharedArea, self.lockFile )
#      
#      result = self.runCommand( 'Flag enabled to forcefully remove current %s' % self.lockFile, cmd, check = True )
#      
#      if not result[ 'OK' ]:
#        self.setApplicationStatus( 'Could Not Remove Lock File' )
#        self.log.warn( result[ 'Message' ] )
#        
#        result[ 'Message' ]     = self.lockFile
#        result[ 'Description' ] = 'Could not remove existing lock file via flag' 
#        result[ 'SamResult' ]   = 'error'
#        
#        return result
#
#      self.setJobParameter( 'ExistingSAMLock', 'Deleted via SAM test flag on %s' % time.asctime() )
#
#    return S_OK()  
#
#  def __checkSAMLockFile( self ):
#    '''
#       Checks SAMLockFile
#    '''
#    
#    self.log.info( '>> __checkSAMLockFile' )
#    
#    lockFilePath = '%s/%s' % ( self.sharedArea, self.lockFile )
#    
#    if not os.path.exists( lockFilePath ):
#      self.log.verbose( "No lock file at %s, nothing to do." % lockFilePath )
#        
#    else:
#      
#      self.log.info( 'Lock file found: %s' % lockFilePath )
#      curtime = time.time()
#      fileTime = os.stat( lockFilePath )[ 8 ]
#      
#      secondsAgo = curtime - fileTime
#      
#      if secondsAgo > self.lockValidity:
#      
#        self.log.info( 'SAM lock file present for > %s secs, deleting' % self.lockValidity )
#        cmd = 'rm -fv %s' % lockFilePath
#        _msg = 'Current lock file %s present for longer than %s seconds'
#        
#        result = self.runCommand(  _msg % ( self.lockFile, self.lockValidity ), cmd, check = True )
#        self.setApplicationStatus( 'Could Not Remove Old Lock File' )
#        
#        if not result[ 'OK' ]:
#          self.log.warn( result[ 'Message' ] )
#          
#          result[ 'Description' ] = 'Could not remove existing lock file exceeding maximum validity' 
#          result[ 'SamResult' ]   = 'error'
#          return result
#        
#        self.setJobParameter( 'ExistingSAMLock', 'Removed on %s after exceeding maximum validity' % ( time.asctime() ) )
#      
#      else:
#        #unique to this test, prevent execution of software installation via 'notice' status
#        _msg = 'ANOTHER SAM job started at this site for %s seconds ago'
#        
#        self.log.info( _msg % secondsAgo )
#        self.writeToLog( _msg % secondsAgo )
#        
#        self.setApplicationStatus( 'Shared Area Lock Exists' )
#                        
#        result = S_ERROR( 'Status NOTICE (= 30)' )
#        result[ 'Description' ] = '%s test running at same time as another SAM job' % self.testName
#        result[ 'SamResult' ]   = 'warning'
#        return result
#
#    return S_OK()
#
#  def __checkSetLockFile( self ):
#    '''
#       Checks Lock file
#    '''
#    
#    self.log.info( '>> __checkSetLockFile' )
#    
#    lockFilePath = '%s/%s' % ( self.sharedArea, self.lockFile )
#    
#    cmd = 'touch %s' % lockFilePath
#    result = self.runCommand( 'Creating SAM lock file', cmd, check = True )
#    
#    if result[ 'OK' ]:
#      
#      self.log.verbose( "Touched %s" % lockFilePath )
#      
#    else:  
#      
#      self.log.warn( result[ 'Message' ] )
#      self.log.info( 'Trying to change permissions: %s' % ( self.sharedArea ) )
#      
#      try:
#        os.chmod( self.sharedArea, 0775 )
#      except OSError:
#        self.setApplicationStatus( 'Could Not Create Lock File' )
#        
#        result = S_ERROR( self.sharedArea )
#        result[ 'Description' ] = 'Could not change permissions'
#        result[ 'SamResult' ]   = 'error' 
#        return result
#      
#      cmd = 'touch %s' % lockFilePath
#      result = self.runCommand( 'Creating SAM lock file (second attempt)', cmd, check = True )
#      
#      if not result[ 'OK' ]:
#        self.setApplicationStatus( 'Could Not Create Lock File' )
#        
#        result = S_ERROR( lockFilePath )
#        result[ 'Description' ] = 'Could not create lock file'
#        result[ 'SamResult' ]   = 'error' 
#        return result
#     
#    return S_OK()  
#
#  def __checkInstallProject( self ):
#    '''
#       Checks InstallProject
#    '''
#    
#    self.log.info( '>> __checkInstallProject' )
#    
#    installProject = '%s/install_project.py' % ( self.sharedArea )
#    if not os.path.exists( installProject ):
#      self.log.verbose( "InstallProject not found at %s, nothing to do." % installProject )
#      
#    else:  
#      self.log.info( 'Removing install_project from SharedArea' )
#      cmd = 'rm -fv %s' % installProject
#      result = self.runCommand( 'Removing install_project from SharedArea', cmd, check = True )
#      
#      if not result[ 'OK' ]:
#        self.setApplicationStatus( 'Could Not Remove File' )
#        self.log.warn( result[ 'Message' ] )
#        
#        result[ 'Description' ] = 'Could not remove install_project from SharedArea'
#        result[ 'SamResult' ]   = 'error' 
#        return result
#    
#    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF