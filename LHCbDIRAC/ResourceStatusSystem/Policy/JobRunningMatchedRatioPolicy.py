''' LHCbDIRAC.ResourceStatusSystem.Policy.JobRunningMatchedRatioPolicy
  
   JobRunningMatchedRatioPolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase
  
'''

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class JobRunningMatchedRatioPolicy( PolicyBase ):
  '''
  The JobRunningMatchedRatioPolicy class is a policy that checks the efficiency of the 
  jobs according to what is on WMS.
  
    Evaluates the JobRunningMatchedRatioPolicy results given by the JobCommand.JobCommand
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

    commandResult = commandResult[ 0 ]

    if not commandResult:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return S_OK( result )

    running  = float( commandResult[ 'Running' ] )
    matched  = float( commandResult[ 'Matched' ] )
    received = float( commandResult[ 'Received' ] )
    checking = float( commandResult[ 'Checking' ] )

    total = running + matched + received + checking

    if not total:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No jobs take a decision'
      return S_OK( result )
    
    efficiency = running / total

    if efficiency < 0.65:
      result[ 'Status' ] = 'Banned'
    elif efficiency < 0.9:
      result[ 'Status' ] = 'Degraded'  
    else:   
      result[ 'Status' ] = 'Active'    
          
    result[ 'Reason' ] = 'Job Running / Matched ratio of %.2f' % efficiency
    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF