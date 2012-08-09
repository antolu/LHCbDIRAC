# $HeadURL$
''' SEOccupancyPolicy

  The SEOccupancyPolicy class is a policy class satisfied when a SE has a 
  high occupancy.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class SEOccupancyPolicy( PolicyBase ):
  '''
  SEOccupancyPolicy, given the space left at the element, proposes a new status.
  '''

  def evaluate(self):
    """
    Evaluate policy on SE occupancy: Use SLS_Command

   :returns:
      {
        'Status':Error|Active|Bad|Banned,
        'Reason': Some lame statements that have to be updated
      }
    """

    # This call SLS_Command/SLSStatus_Command (see Configurations.py)
    commandResult = super( SEOccupancyPolicy, self ).evaluate()
    result = {}

    if commandResult is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return result

    # SLSStatus_Command returns None if something goes wrong,
    # otherwise returns an integer (the SLS availability)

    commandResult = commandResult[ 'Value' ]
    # FIXME: Use threshold from SLS, put more meaningful comments.
    if commandResult == 0: 
      result[ 'Status' ] = 'Banned'
      comment            = 'SE Full!'
    elif commandResult < 10: 
      result[ 'Status' ] = 'Bad'
      comment            = 'SE has not much space left'  
    else: 
      result[ 'Status' ] = 'Active'
      comment            = 'SE has enough space left'

    result['Reason'] = 'Space availability: %d %% (%s)' % ( commandResult, comment )
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  
