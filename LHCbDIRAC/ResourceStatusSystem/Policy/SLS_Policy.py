########################################################################
# $HeadURL:
########################################################################

""" The SLS_Policy class is a policy class satisfied when a SLS sensors report problems
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SLS_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on SLS availability. Use SLS_Command/SLSStatus_Command.

   :returns:
      {
        'Status':Error|Unknown|Active|Banned,
        'Reason':'Availability:High'|'Availability:Mid-High'|'Availability:Low',
      }
    """

    # Execute the command and returns a value as a string.
    status = super(SLS_Policy, self).evaluate()
    result = {}

    if not status[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = status[ 'Message' ]
      return result
   
    status = status[ 'Value' ]
    # FIXME: Should get thresholds from SLS !!!!
    if status < 40: 
      result[ 'Status' ] = 'Banned' 
      comment            = 'Poor'
    elif status < 90:
      result[ 'Status' ] = 'Bad'
      comment            = 'Low'
    else:   
      result[ 'Status' ] = 'Active' 
      comment            = 'High'

    result[ 'Reason' ] = 'SLS availability: %d %% (%s)' % ( status, comment )
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
