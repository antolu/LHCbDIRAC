''' LHCb System Configuration SAM Test Module

  Corresponds to SAM test CE-lhcb-os.
'''

import os

from DIRAC import S_OK, S_ERROR, gConfig

from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM  import ModuleBaseSAM
from LHCbDIRAC.SAMSystem.Utilities              import Utils

__RCSID__ = "$Id$"

class SystemConfiguration( ModuleBaseSAM ):

#  def __init__( self ):
#    '''
#        Standard constructor for SAM Module
#    '''
#    
#    ModuleBaseSAM.__init__( self )
#    
#    #self.logFile  = 'sam-os.log'
#    #self.testName = 'CE-lhcb-os'

  def _execute( self ):
    '''
       The main execution method of the SystemConfiguration module.
       Checks:
        - proxy mapping
        - local root
        - shared area
        - proxy
        - accounts
        - rpms
    '''
    
    result = self.__checkMapping()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    self.__checkLocalRoot()
    
    result = self.__checkArchitecture()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    
#FIXME: when does this really apply ??    
#    result = self.__checkRPMs()
#    if not result[ 'OK' ]:
#      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  ##############################################################################
  # Protected methods

  def __checkMapping( self ):
    '''
       Return warning if the mapping is not the one expected..
    '''

    self.log.info( '>> __checkMapping' )
    
    proxy         = self.runInfo.get( 'Proxy', '' )
    identityShort = self.runInfo.get( 'identityShort' )
    
    result, _description = None, ''
    
    if "lcgadmin" in proxy.lower():
      
      #FIXME: What the fuck are we looking for here ???
      if identityShort.find( 's' ) != -1 or identityShort.find( 'g' ) != -1 or identityShort.find( 'm' ) != -1:
        self.log.info( 'correct mapping with proxy for "lcgadmin"' )
        return S_OK()
      else:
        _description = "Not found [s|g|m] for 'lcgadmin' on %s" % identityShort
        self.log.warn( _description )
        result = S_ERROR( identityShort )
            
#FIXME: should we check this for usual SAM jobs ?            
#    elif "production" in proxy.lower():
#    
#      #FIXME: What the fuck are we looking for here ???
#      if identityShort.find( 'p' ) != -1 or identityShort.find( 'r' ) != -1 or identityShort.find( 'd' ) != -1:
#        self.log.info( 'correct mapping with proxy for "production"' )
#        result = S_OK()
#      else:
#        _description = "Not found [p|r|d] for 'production' on %s" % identityShort
#        self.log.warn( _description )
#        result = S_ERROR( identityShort )
        
    else:
      #_description = "Neither 'lcgadmin' nor 'production' found on Proxy"
      _description = "'lcgadmin' not found on Proxy"
      self.log.warn( _description )
      result = S_ERROR( proxy )
      
    if result[ 'OK' ]:
      return result
    
    result[ 'Description' ] = _description
    result[ 'SamResult' ]   = 'warning'
    
    return result  

  def __checkLocalRoot( self ):
    '''
       Checks local root
    '''
       
    self.log.info( '>> __checkLocalRoot' )   
        
    localRoot = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
    self.log.info( "Root directory for job is %s" % localRoot )
    
    return S_OK()

  def __checkArchitecture( self ):
    '''
       Checks current platform and compares it with the supported ones. Then checks
       compatibility of libraries with that architecture.
    '''
    
    self.log.info( '>> __checkArchitecture' )

    systemConfigs = Utils.getLocalArchitecture()
    if not systemConfigs[ 'OK' ]:
      self.log.error( systemConfigs[ 'Message' ] )
      return systemConfigs
    systemConfigs = systemConfigs[ 'Value' ]
    self.log.info( 'Current system configurations are: %s ' % ( ', '.join( systemConfigs ) ) )

    compatiblePlatforms = Utils.getLocalPlatforms()
    if not compatiblePlatforms[ 'OK' ]:
      return compatiblePlatforms    
    cPlats = compatiblePlatforms[ 'Value' ].keys()
    
    compatible = False
    for sc in systemConfigs:
      if sc in cPlats:
        compatible = True
        break
      
    if not compatible:
      
      self.log.error( 'There is no compatibility between Architecture and OS' )
      self.log.error( 'SystemConfigs: %s' % ','.join( systemConfigs ) )
      self.log.error( 'OSCompatibility: %s' % ','.join( compatiblePlatforms ) )
      
      result = S_ERROR( ', '.join( systemConfigs ) )
      result[ 'Description' ] = '%s%s' %( str( cPlats ), str( systemConfigs ) )
      result[ 'SamResult' ]   = 'error'
      return result

#FIXME: this fails in most cases ( CERNVMFS ! ), but on production runs with check = False,
#what is the poing of that ?
#    for arch in systemConfigs:
#      libPath = '%s/%s/' % ( self.sharedArea, arch )
#      cmd     = 'ls -alR %s' % libPath
#      # We add True, if there is not such directory, we should not continue
#      result  = self.runCommand( 'Checking compatibility libraries for system configuration %s' % ( arch ), 
#                                 cmd, check = True )
#      if not result[ 'OK' ]:
#        
#        result[ 'Description' ] = 'Failed to check compatibility library directory %s' % libPath
#        result[ 'SamResult' ]   = 'error'
#        
#        return result
        
    return S_OK()

  def __checkRPMs( self ):
    '''
       Checks RPM version of LCG Utils
    '''
    
    self.log.info( '>> __checkRPMs' )
    
    cmd = 'rpm -qa | grep lcg_util | cut -f 2 -d "-"'
    rpmOutput = self.runCommand( "Checking RPM for LCG Utilities", cmd, check = True )
    if not rpmOutput[ 'OK' ]:
      self.log.error( 'Error getting RPM version matches for "lcg_util"' )
      rpmOutput[ 'Description' ] = 'Could not get RPM version'
      rpmOutput[ 'SamResult' ]   = 'error'
      return rpmOutput
    rpmOutput = rpmOutput[ 'Value' ]
    
    if not rpmOutput:
      self.log.error( 'Could not get RPM version matches for "lcg_util"' )
      result = S_ERROR( rpmOutput )
      result[ 'Description' ] = 'Could not get RPM version matches for "lcg_util"'
      result[ 'SamResult' ]   = 'error'
      return result    
    elif rpmOutput.split( '.' )[ 0 ] == '1' and int( rpmOutput.split( '.' )[ 1 ] ) < 6:
      self.log.error( 'RPM version is not correct %s' % rpmOutput )  
      result = S_ERROR( rpmOutput )
      result[ 'Description' ] = 'RPM version not correct'
      result[ 'SamResult' ]   = 'error'
      return result    
    
    return S_OK()
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
  
#FIXME: Unused... study whether it makes sense or not to put it back
#  def __deleteSharedAreaFiles( self, sharedArea, filePattern ):
#    """Remove all files in shared area.
#    """
#    self.log.verbose( 'Removing all files with name %s in shared area %s' % ( filePattern, sharedArea ) )
#    self.writeToLog( 'Removing all files with name %s shared area %s' % ( filePattern, sharedArea ) )
#    count = 0
#    try:
#      globList = glob.glob( '%s/%s' % ( sharedArea, filePattern ) )
#      for check in globList:
#        if os.path.isfile( check ):
#          os.remove( check )
#          count += 1
#    except OSError, x:
#      self.log.error( 'Problem deleting shared area ', str( x ) )
#      return S_ERROR( x )
#
#    if count:
#      self.log.info( 'Removed %s files with pattern %s from shared area' % ( count, filePattern ) )
#      self.writeToLog( 'Removed %s files with pattern %s from shared area' % ( count, filePattern ) )
#    else:
#      self.log.info( 'No %s files to remove' % filePattern )
#      self.writeToLog( 'No %s files to remove' % filePattern )
#
#    self.log.info( 'Shared area %s successfully purged of %s files' % ( sharedArea, filePattern ) )
#    self.writeToLog( 'Shared area %s successfully purged of %s files' % ( sharedArea, filePattern ) )
#    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF