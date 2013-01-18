''' LHCbDIRAC.ResourceStatusSystem.Policy.SpaceTokenOccupancyPolicy

   SpaceTokenOccupancyPolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase
  
'''

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class SpaceTokenOccupancyPolicy( PolicyBase ):
  '''
  The SpaceTokenOccupancyPolicy class is a policy class satisfied when a SE has a 
  high occupancy.

  SpaceTokenOccupancyPolicy, given the space left at the element, proposes a new status.
  '''

  @staticmethod
  def _evaluate( commandResult ):
    """
    Evaluate policy on SE occupancy: Use SLS_Command

   :returns:
      {
        'Status':Error|Active|Bad|Banned,
        'Reason': Some lame statements that have to be updated
      }
    """

    result = {}

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return S_OK( result )

    commandResult = commandResult[ 'Value' ]

    if not commandResult:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return S_OK( result )

    if 'total' not in commandResult.keys(): 
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Key total missing'
      return S_OK( result )

    if 'free' not in commandResult.keys(): 
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Key free missing'
      return S_OK( result )
    
    if 'guaranteed' not in commandResult.keys(): 
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Key guaranteed missing'
      return S_OK( result )
    
    percentage = ( commandResult[ 'free' ] / commandResult[ 'total' ] ) * 100
    
    # FIXME: Use threshold from SLS, put more meaningful comments.
    if percentage == 0: 
      result[ 'Status' ] = 'Banned'
      comment            = 'SE Full!'
    elif percentage < 10: 
      result[ 'Status' ] = 'Degraded'
      comment            = 'SE has not much space left'  
    else: 
      result[ 'Status' ] = 'Active'
      comment            = 'SE has enough space left'

    result[ 'Reason' ] = 'Space availability: %d (%s)' % ( percentage, comment )
    return S_OK( result )
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  