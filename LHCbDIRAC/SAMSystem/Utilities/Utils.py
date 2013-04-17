''' Utils
 
  functions used on the SAMModules, which are then pushed to workflow_commons

'''
#import os, re
#
#from DIRAC import S_OK, S_ERROR, gLogger, siteName, gConfig
#
#from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation import createSharedArea, getSharedArea
#
#__RCSID__ = '$Id$'
#
#################################################################################
#
#def getMessageString( message, header = False ):
#  '''
#     Return a nicely formatted string for the SAM logs.
#  '''
#    
#  # Get the length of the longest string
#  limit  = 0
#  for line in message.split( '\n' ):
#    limit = max( limit, len( line ) )
#
#  #Max length of 100 characters
#  limit = min( limit, 100 )
#    
#  if header:
#    separator = '='
#  else:
#    separator = '-'
#        
#  border = separator * limit
#        
#  if header:
#    message = '\n%s\n%s\n%s\n' % ( border, message, border )
#  else:
#    message = '%s\n%s\n%s\n' % ( border, message, border )
#      
#  return message
#  
#################################################################################
## SharedArea checks
#
#def checkSharedArea( log = None ):
#  '''
#    Tries to get sharedArea. If it fails, attempts to create a new one.
#  '''
# 
#  if log is None:
#    log = gLogger
#
#  sharedArea = getSharedArea()
#  if not sharedArea:
#    
#    log.info( 'Could not determine sharedArea for site %s' % siteName() ) 
#    log.info( 'trying to create it...' )
#    
#    if not createSharedArea():
#      result = S_ERROR( sharedArea )
#      result[ 'Description' ] = 'Could not create sharedArea for site %s:' % siteName()
#      result[ 'SamResult' ]   = 'error'
#      return result
#    
#    sharedArea = getSharedArea()
#    if not sharedArea or not os.path.exists( sharedArea ):
#      # After previous check this error should never occur
#      log.info( 'Could not determine sharedArea for site %s:\n%s' % ( siteName(), sharedArea ) )
#      result = S_ERROR( sharedArea )
#      result[ 'Description' ] = 'Could not determine shared area for site'
#      result[ 'SamResult' ]   = 'error' 
#      
#      return result
#    
#  else:
#    log.info( 'Software shared area for site %s is %s' % ( siteName(), sharedArea ) )
#
#  return S_OK( sharedArea )
#
#def checkCernVMFS( sharedArea, log = None ):
#  '''
#    Checks if sharedArea path is a CernVMFS path "path/etc/cernvmfs", if so, it 
#    returns S_ERROR
#  '''
#
#  if log is None:
#    log = gLogger
#    
#  if os.path.exists( os.path.join( sharedArea, 'etc', 'cernvmfs' ) ):
#    log.info( 'Software shared area for site %s is using CERNVMFS' % ( siteName() ) )
#    result = S_ERROR( 'Read-Only volume' )
#    result[ 'Description' ] = 'Could not install (CERNVMFS) for site %s:' % siteName()
#    result[ 'SamResult' ]   = 'warning'
#    return result  
#  
#  return S_OK()
#
##FIXME: does this apply ?
##def fixWritableVolume( sharedArea, log = None ):
##  '''
##    Small hack to change path to writable volume at CERN
##  '''  
##    
##  if log is None:
##    log = gLogger  
##    
##  #nasty fix but only way to resolve writeable volume at CERN
##  if siteName() in ( 'LCG.CERN.ch', 'LCG.CERN5.ch' ):
##    log.info( 'Changing shared area path to writable volume at CERN' )
##    if re.search( '.cern.ch', sharedArea ):
##      sharedArea = sharedArea.replace( 'cern.ch', '.cern.ch' )
##      os.environ[ 'VO_LHCB_SW_DIR' ] = os.environ[ 'VO_LHCB_SW_DIR' ].replace( 'cern.ch', '.cern.ch' )
##
##  elif siteName() in ( 'LCG.IN2P3.fr', 'LCG.IN2P3-T2.fr' ):
##    log.info( 'Changing shared area path to writeable volume at IN2P3' )
##    if re.search( '.in2p3.fr', sharedArea ):
##      sharedArea = sharedArea.replace( 'in2p3.fr', '.in2p3.fr' )
##      os.environ[ 'VO_LHCB_SW_DIR' ] = os.environ[ 'VO_LHCB_SW_DIR' ].replace( 'in2p3.fr', '.in2p3.fr' )
##      
##  return S_OK( sharedArea )
#
#################################################################################
## Architecture checks
#
#def getLocalArchitecture():
#  '''
#    Get local architecture
#  '''
#
#  localArch = gConfig.getValue( '/LocalSite/Architecture', [] )
#  if not localArch:
#            
#    result = S_ERROR( 'Could not get /LocalSite/Architecture' )
#    result[ 'Description' ] = '/LocalSite/Architecture is not defined in the local configuration' 
#    result[ 'SamResult' ]   = 'error'
#      
#    return result
#
#  return S_OK( localArch )
#
#def getLocalPlatforms():
#  '''
#     Get local platforms
#  '''
#  
#  #must get the list of compatible platforms for this architecture
#  localPlatforms = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
#  if not localPlatforms:
#    _msg = 'Could not obtain compatible platforms for /Resources/Computing/OSCompatibility/'
#    result = S_ERROR( 'Could not get /Resources/Computing/OSCompatibility' )
#    result[ 'Description' ] = '/Resources/Computing/OSCompatibility is not defined in the local configuration' 
#    result[ 'SamResult' ]   = 'error'
#      
#    return result
#
#  return localPlatforms

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF