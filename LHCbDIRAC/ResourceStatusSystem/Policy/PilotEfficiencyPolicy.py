''' LHCbDIRAC.ResourceStatusSystem.Pilot.PilotEfficiencyPolicy
  
  PilotEfficiencyPolicy.__bases__:
    DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase
  
'''

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id: $'

class PilotEfficiencyPolicy( PolicyBase ):
  '''
    The PilotEfficiencyPolicy class is a policy that checks the efficiency of the 
    pilots according to what is on the accounting.

    Evaluates the PilotEfficiency results given by the JobCommand.JobCommand
  '''
  
  @staticmethod
  def _evaluate( commandResult ):
    '''
    Evaluate policy on pilot stats, using args (tuple).

    :returns:
      {
        'Status':Unknown|Active|Probing|Bad,
        'Reason':'JobsEff:Good|JobsEff:Fair|JobsEff:Poor|JobsEff:Bad|JobsEff:Idle',
      }
    '''

    result = { 
              'Status' : None,
              'Reason' : None
              }

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return S_OK( result )

    commandResult = commandResult[ 'Value' ]

    if not commandResult:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return S_OK( result )

    commandResult = commandResult[ 0 ]

    if not commandResult:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return S_OK( result )

    aborted = float( commandResult[ 'Aborted' ] )
    deleted = float( commandResult[ 'Deleted' ] )
    done    = float( commandResult[ 'Done' ] )
    failed  = float( commandResult[ 'Failed' ] )

    total     = aborted + deleted + done + failed

    if not total:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No pilots to take a decision'
      return S_OK( result )
    
    efficiency = done / total

    if efficiency < 0.65:
      result[ 'Status' ] = 'Banned'
    elif efficiency < 0.90:
      result[ 'Status' ] = 'Degraded'  
    else:   
      result[ 'Status' ] = 'Active'    
          
    result[ 'Reason' ] = 'Pilots Efficiency of %.2f' % efficiency
    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF