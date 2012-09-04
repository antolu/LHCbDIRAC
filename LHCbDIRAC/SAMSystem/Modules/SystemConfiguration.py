# $HeadURL$
''' LHCb System Configuration SAM Test Module

  Corresponds to SAM test CE-lhcb-os.
'''

import os
import re

import DIRAC

from DIRAC import S_OK, S_ERROR, gConfig

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSharedArea
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM              import ModuleBaseSAM

__RCSID__ = "$Id$"

class SystemConfiguration( ModuleBaseSAM ):

  def __init__( self ):
    '''
        Standard constructor for SAM Module
    '''
    
    ModuleBaseSAM.__init__( self )
    
    self.logFile  = 'sam-os.log'
    self.testName = 'CE-lhcb-os'

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
    
    result = self.__checkMapping( self.runInfo[ 'Proxy' ], self.runInfo[ 'identityShort' ] )
    if not result[ 'OK' ]:
      return self.finalize( 'Potential problem in the mapping', self.runInfo[ 'identityShort' ], 'warning' )

    self.__checkLocalRoot()
    
    result = self.__checkSharedArea()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    sharedArea = result[ 'Value' ]

    result = self.__checkProxy()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    self.__checkAccounts()
    
    result = self.__checkArchitecture( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    
    result = self.__checkRPMs()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  ##############################################################################
  # Protected methods

  def __checkMapping( self, proxy, map_name ):
    '''
       Return warning if the mapping is not the one expected..
    '''

    self.log.info( ' Check mapping' )
    _errorMsg = 'potential problem in the mapping'
    
    if proxy.find( 'lcgadmin' ) != -1:
      
      if map_name.find( 's' ) != -1 or map_name.find( 'g' ) != -1 or map_name.find( 'm' ) != -1:
        self.log.info( 'correct mapping' )
        return S_OK()
      else:
        self.log.warn( _errorMsg )
        return S_ERROR( _errorMsg )
    
    elif proxy.lower().find( 'production' ) != -1:
    
      if map_name.find( 'p' ) != -1 or map_name.find( 'r' ) != -1 or map_name.find( 'd' ) != -1:
        self.log.info( 'correct mapping' )
        return S_OK()
      else:
        self.log.warn( _errorMsg )
        return S_ERROR( _errorMsg )
    
    else:
      self.log.warn( _errorMsg )
      return S_ERROR( _errorMsg )

  def __checkLocalRoot( self ):
    '''
       Checks local root
    '''
        
    localRoot = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
    self.log.info( "Root directory for job is %s" % ( localRoot ) )
    
    return S_OK()

  def __checkSharedArea( self ):
    '''
       Determines shared area and performs few basic operations
    '''

    sharedArea = getSharedArea()
    
    if not sharedArea or not os.path.exists( sharedArea ):
      self.log.info( 'Could not determine sharedArea for site %s:\n%s' % ( DIRAC.siteName(), sharedArea ) )
      
      result = S_ERROR( sharedArea )
      result[ 'Description' ] = 'Could not determine shared area for site'
      result[ 'SamResult' ] = 'critical'
      return result
      #return self.finalize( 'Could not determine shared area for site', sharedArea, 'critical' )
    else:
      self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )

    #nasty fix but only way to resolve writeable volume at CERN
    if DIRAC.siteName() == 'LCG.CERN.ch':
      self.log.info( 'Changing shared area path to writeable volume at CERN' )
      if re.search( '.cern.ch', sharedArea ):
        newSharedArea = sharedArea.replace( 'cern.ch', '.cern.ch' )
        self.writeToLog( 'Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s' % ( sharedArea,
                                                                                                       newSharedArea ) )
        sharedArea = newSharedArea

    self.log.info( 'Checking shared area contents: %s' % ( sharedArea ) )
    result = self.runCommand( 'Checking contents of shared area directory: %s' % sharedArea, 'ls -al %s' % sharedArea )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not list contents of shared area'
      result[ 'SamResult' ]   = 'error'
      #return self.finalize( 'Could not list contents of shared area', result[ 'Message' ], 'error' )
      return result

    self.log.verbose( 'Trying to resolve shared area link problem' )
    if os.path.exists( '%s/lib' % sharedArea ):
      if os.path.islink( '%s/lib' % sharedArea ):
        self.log.info( 'Removing link %s/lib' % sharedArea )
        result = self.runCommand( 'Removing link in shared area', 'rm -fv %s/lib' % sharedArea, check = True )
        if not result[ 'OK' ]:
          result[ 'Description' ] = 'Could not remove link in shared area'
          result[ 'SamResult' ]   = 'error'
          return result
          #return self.finalize( 'Could not remove link in shared area', result['Message'], 'error' )
      else:
        self.log.info( '%s/lib is not a link so will not be removed' % sharedArea )
    else:
      self.log.info( 'Link in shared area %s/lib does not exist' % sharedArea )

    self.log.info( 'Checking shared area contents: %s' % ( sharedArea ) )
    result = self.runCommand( 'Checking contents of shared area directory: %s' % sharedArea, 'ls -al %s' % sharedArea )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not list contents of shared area'
      result[ 'SamResult'   ] = 'error'
      return result
      #return self.finalize( 'Could not list contents of shared area', result['Message'], 'error' )
    
    return S_OK( sharedArea )

  def __checkProxy( self ):
    '''
       Checks proxy
    '''
    
    result = self.runCommand( 'Checking current proxy', 'voms-proxy-info -all' )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'voms-proxy-info -all'
      result[ 'SamResult' ]   = 'error'
      
    return result  
      
  def __checkAccounts( self ):
    '''
       Checks the type of account used on the site
    '''
        
    self.log.info( 'Current account: %s' % self.runInfo[ 'identity' ] )
    
    _msg = '%s uses pool accounts' 
    if not re.search( '\d', self.runInfo[ 'identityShort' ] ):
      _msg = '%s uses static accounts' 
    self.log.info( _msg % DIRAC.siteName() )  
    
    return S_OK()    

  def __checkArchitecture( self, sharedArea ):
    '''
       Checks current platform and compares it with the supported ones. Then checks
       compatibility of libraries with that architecture.
    '''
    
    systemConfigs = gConfig.getValue( '/LocalSite/Architecture', [] )
    self.log.info( 'Current system configurations are: %s ' % ( ', '.join( systemConfigs ) ) )
    
    compatiblePlatforms = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
    if not compatiblePlatforms[ 'OK' ]:
      compatiblePlatforms[ 'Description' ] = 'Could not establish compatible platforms'
      compatiblePlatforms[ 'SamResult' ]   = 'error'
      return compatiblePlatforms
      #return self.finalize( 'Could not establish compatible platforms', compatiblePlatforms[ 'Message' ], 'error' )
    
    cPlats = compatiblePlatforms[ 'Value' ].keys()
    
    compatible = False
    for sc in systemConfigs:
      if sc in cPlats:
        compatible = True
        break
      
    if not compatible:
      result = S_ERROR( ', '.join( systemConfigs ) )
      result[ 'Description' ] = 'Site does not have an officially compatible platform'
      result[ 'SamResult' ]   = 'critical'
      return result
#      return self.finalize( 'Site does not have an officially compatible platform',
#                            ', '.join( systemConfigs ), 'critical' )

    for arch in systemConfigs:
      libPath = '%s/%s/' % ( sharedArea, arch )
      cmd     = 'ls -alR %s' % libPath
      result  = self.runCommand( 'Checking compatibility libraries for system configuration %s' % ( arch ), cmd )
      if not result[ 'OK' ]:
        
        result[ 'Description' ] = 'Failed to check compatibility library directory %s' % libPath
        result[ 'SamResult' ]   = 'error'
        
        return result
        
#        return self.finalize( 'Failed to check compatibility library directory %s' % libPath,
#                              result['Message'], 'error' )    
    return S_OK()

  def __checkRPMs( self ):
    '''
       Checks RPM version of LCG Utils
    '''
    
    cmd = 'rpm -qa | grep lcg_util | cut -f 2 -d "-"'
    result = self.runCommand( 'Checking RPM for LCG Utilities', cmd )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not get RPM version'
      result[ 'SamResult' ]   = 'error'
      return result
      #return self.finalize( 'Could not get RPM version', result['Message'], 'error' )

    rpmOutput = result[ 'Value' ]
    if rpmOutput.split( '.' )[ 0 ] == '1':
      if int( rpmOutput.split( '.' )[ 1 ] ) < 6:
        result = S_ERROR( rpmOutput )
        result[ 'Description' ] = 'RPM version not correct'
        result[ 'SamResult' ]   = 'warning'
        return result
        #return self.finalize( 'RPM version not correct', rpmOutput, 'warning' )    
    
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