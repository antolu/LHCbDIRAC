# $HeadURL$
''' SEOccupancyPolicy

  The SEOccupancyPolicy class is a policy class satisfied when a SE has a 
  high occupancy.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class SEOccupancyPolicy( PolicyBase ):

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
    status = super( SEOccupancyPolicy, self ).evaluate()
    result = {}

    if status is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not status[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = status[ 'Message' ]
      return result

    # SLSStatus_Command returns None if something goes wrong,
    # otherwise returns an integer (the SLS availability)

    status = status[ 'Value' ]
    # FIXME: Use threshold from SLS, put more meaningful comments.
    if status == 0: 
      result[ 'Status' ] = 'Banned'
      comment            = 'SE Full!'
    elif status < 10: 
      result[ 'Status' ] = 'Bad'
      comment            = 'SE has not much space left'  
    else: 
      result[ 'Status' ] = 'Active'
      comment            = 'SE has enough space left'

    result['Reason'] = 'Space availability: %d %% (%s)' % ( status, comment )
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  
