# $HeadURL$
''' JobsEfficiencySimplePolicy
  
  The JobsEfficiencySimplePolicy class is a policy class
  that checks the efficiency of the pilots.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class JobsEfficiencySimplePolicy( PolicyBase ):

  def evaluate( self ):
    """
    Evaluate policy on jobs stats, using args (tuple).

    :returns:
      {
        'Status':Unknown|Active|Probing|Bad,
        'Reason':'JobsEff:Good|JobsEff:Fair|JobsEff:Poor|JobsEff:Bad|JobsEff:Idle',
      }
  """

    commandResult = super( JobsEfficiencySimplePolicy, self ).evaluate()
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
      
    if commandResult == 'Good':
      result[ 'Status' ] = 'Active'
    elif commandResult == 'Fair':
      result[ 'Status' ] = 'Active'   
    elif commandResult == 'Poor':
      result[ 'Status' ] = 'Probing'
    elif commandResult == 'Idle':
      result[ 'Status' ] = 'Unknown'
    elif commandResult == 'Bad':
      result[ 'Status' ] = 'Bad'

    result[ 'Reason' ] = 'Simple Jobs Efficiency: '
    if commandResult != 'Idle':
      result[ 'Reason' ] = result[ 'Reason' ] + commandResult
    else:
      result[ 'Reason' ] = 'No values to take a decision'

    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF