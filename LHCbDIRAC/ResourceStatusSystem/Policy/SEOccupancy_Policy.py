########################################################################
# $HeadURL:
########################################################################

""" The SEOccupancy_Policy class is a policy class satisfied when a SE has a high occupancy
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SEOccupancy_Policy(PolicyBase):

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
    status = super(SEOccupancy_Policy, self).evaluate()
    result = {}

    if not status[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = status[ 'Message' ]
      return result

    # SLSStatus_Command returns None if something goes wrong,
    # otherwise returns an integer (the SLS availability)

    #elif status == 'Unknown':
    #  return { 'Status' : 'Unknown' }
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
