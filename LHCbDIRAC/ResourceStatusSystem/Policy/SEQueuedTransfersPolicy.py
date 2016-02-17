''' LHCbDIRAC.ResourceStatusSystem.Policy.SEQueuedTransfersPolicy

   SEQueuedTransfersPolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = "$Id$"

#...............................................................................
#
#
# OBSOLETE CODE. TODO: refactor it !
#
#...............................................................................

class SEQueuedTransfersPolicy( PolicyBase ):
  '''
  The SEQueuedTransfersPolicy class is a policy class satisfied when a SE has 
  a high number of queued transfers.

  SEQueuedTransfersPolicy, given the amount of queued transfers on the element,
  proposes a new status.
  '''

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

    commandResult = super( SEQueuedTransfersPolicy, self ).evaluate()
    result = {}

    if commandResult is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return result
    
    commandResult = commandResult[ 'Value' ]
    # type float, but represent an int, no need to round.
    commandResult = int( commandResult[ 'Queued transfers' ] ) 

    if commandResult < 70: 
      result['Status'] = 'Active'
      comment          = 'low'
    elif commandResult < 100: 
      result['Status'] = 'Bad'
      comment          = 'mid-high'
    else:
      result['Status'] = 'Banned'
      comment          = 'high'
    
    result[ 'Reason' ] = 'Queued transfers on the SE: %d (%s)' % ( commandResult, comment )
    return result

#...............................................................................
#EOF
