"""
Bookkeeping utilities
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_ERROR
from DIRAC.FrameworkSystem.Client.NotificationClient  import NotificationClient
import errno

_IGNORE_PARAMETERS = ['ReplicaFlag', 'Visible']

# The following parameters can not used to build the query, it requires at least one more parameter.
_ONE = ['FileType', 'ProcessingPass', 'EventType', 'DataQuality', 'ConfigName', 'ConfigVersion', 'ConditionDescription'] 

# Two parameter in the list not enough to build the query.
_TWO = ['ConfigName', 'ConfigVersion', 'ConditionDescription', 'ProcessingPass', 'FileType', 'DataQuality'] 

def enoughParams( in_dict ):
  """
  Dirty method to check the query parameters and make sure the queries have enough parameters.
  """
  checkingDict = in_dict.copy()
  if len( checkingDict ) == 0:
    return False
  for param in _IGNORE_PARAMETERS:
    if param in checkingDict:
      checkingDict.pop( param )
  
  if len( checkingDict ) == 0:
    return False
  
  if len( checkingDict ) == 1: 
    for i in checkingDict:
      if i in _ONE:
        return False
  
  if len ( checkingDict ) == 2:
    if not set( checkingDict.keys() ) - set( _TWO ):
      return False
  return True
    
def checkArguments( func ):
  
  def checkMethodArguments( self, *args, **kwargs ):
    """
    This is used to check the conditions of a given query. We assume a dictionary can not be empty and 
    it has more than one element, if we do not take into account the replica flag and the visibility flag
    """
        
    arguments = args[0]
    if not enoughParams( arguments ):
      
      funcName = func.func_name
      res = self.getRemoteCredentials()
      userName = res.get( 'username', 'UNKNOWN' )
      address = 'zmathe@cern.ch'
      subject = '%s method!' % funcName
      body = '%s user has not provided enough input parameters! \n \
              the input parameters:%s ' % ( userName, str( arguments ) )
      NotificationClient().sendMail( address, subject, body, 'zmathe@cern.ch' )
      gLogger.error( 'Got you: %s ---> %s' % ( userName, str( arguments ) ) )
      
      return S_ERROR( errno.EINVAL, "Provide more parameters %s" % str( arguments ) )
    else:
      result = func( self, *args )
      return result
  return checkMethodArguments
