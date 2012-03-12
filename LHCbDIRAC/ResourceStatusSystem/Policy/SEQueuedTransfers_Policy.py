# $HeadURL$
''' SEQueuedTransfers_Policy

  The SEQueuedTransfers_Policy class is a policy class satisfied when a SE has 
  a high number of queued transfers.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class SEQueuedTransfers_Policy( PolicyBase ):

  def evaluate(self):
    """
    Evaluate policy on SE Queued Transfers. Use
    SLS_Command/SLSServiceInfo_Command.  Result of the command is a
    dictionary with SLS infos, or None in case of failure.

    :returns:
        {
          'Status':Error|Active|Bad|Banned,
          'Reason': high, low, mid-high
        }
    """

    status = super(SEQueuedTransfers_Policy, self).evaluate()
    result = {}

    if status is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not status[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = status[ 'Message' ]
      return result
    
    status = status[ 'Value' ]
    # type float, but represent an int, no need to round.
    status = int( status[ 'Queued transfers' ] ) 

    if status < 70: 
      result['Status'] = 'Active'
      comment          = 'low'
    elif status < 100: 
      result['Status'] = 'Bad'
      comment          = 'mid-high'
    else:
      result['Status'] = 'Banned'
      comment          = 'high'
    
    result[ 'Reason' ] = 'Queued transfers on the SE: %d (%s)' % ( status, comment )
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF