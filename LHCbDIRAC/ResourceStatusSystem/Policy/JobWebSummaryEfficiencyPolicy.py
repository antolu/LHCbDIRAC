# $HeadURL:  $
''' JobEfficiencyPolicy
  
  The JobEfficiencyPolicy class is a policy that checks the efficiency of the 
  jobs according to what is on WMS.
  
'''

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id:  $'

class JobWebSummaryEfficiencyPolicy( PolicyBase ):
  '''
    Evaluates the JobEfficiency results given by the JobCommand.JobCommand
  '''
  
  @staticmethod
  def _evaluate( commandResult ):
    '''
    Evaluate policy on jobs stats, using args (tuple).

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

    # The command returns a list of dictionaries, with only one if thre is something,
    # otherwise an empty list.
    commandResult = commandResult[ 0 ]
      
    if 'Status' not in commandResult:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = '"Status" key missing'
      return S_OK( result )  
    
    if 'Efficiency' not in commandResult:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = '"Efficiency" key missing'
      return S_OK( result )  
          
    status     = commandResult[ 'Status' ]
    efficiency = commandResult[ 'Efficiency' ]      
          
    if status == 'Good':
      result[ 'Status' ] = 'Active'
    elif status == 'Fair':
      result[ 'Status' ] = 'Active'   
    elif status == 'Poor':
      result[ 'Status' ] = 'Degraded'
    elif status == 'Idle':
      result[ 'Status' ] = 'Unknown'
    elif status == 'Bad':
      result[ 'Status' ] = 'Banned'
    else:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Unknown status "%s"' % status
      return S_OK( result )   

    result[ 'Reason' ] = 'Jobs Efficiency: %s with status %s' % ( efficiency, status )

    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF