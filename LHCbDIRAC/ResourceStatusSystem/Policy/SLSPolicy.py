''' LHCbDIRAC.ResourceStatusSystem.Policy.SLSPolicy

   SLSPolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

#...............................................................................
#
#
# OBSOLETE CODE. TODO: refactor it !
#
#...............................................................................

class SLSPolicy( PolicyBase ):
  '''
  The SLSPolicy class is a policy class satisfied when a SLS sensors 
  report problems.

  SLSPolicy, given the SLS availability metrics for the element, proposes a new
  status. 
  '''

  def evaluate( self ):
    """
    Evaluate policy on SLS availability. Use SLS_Command/SLSStatus_Command.

   :returns:
      {
        'Status':Error|Unknown|Active|Banned,
        'Reason':'Availability:High'|'Availability:Mid-High'|'Availability:Low',
      }
    """

    # Execute the command and returns a value as a string.
    commandResult = super( SLSPolicy, self ).evaluate()
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
    # FIXME: Should get thresholds from SLS !!!!
    if commandResult < 40: 
      result[ 'Status' ] = 'Banned' 
      comment            = 'Poor'
    elif commandResult < 90:
      result[ 'Status' ] = 'Bad'
      comment            = 'Low'
    else:   
      result[ 'Status' ] = 'Active' 
      comment            = 'High'

    result[ 'Reason' ] = 'SLS availability: %d %% (%s)' % ( commandResult, comment )
    return result
  
#...............................................................................
#EOF
