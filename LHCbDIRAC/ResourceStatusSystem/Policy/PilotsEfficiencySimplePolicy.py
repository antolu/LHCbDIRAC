# $HeadURL$
''' PilotsEfficiencySimplePolicy

  The PilotsEfficiencySimplePolicy class is a policy class
  that checks the efficiency of the pilots.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class PilotsEfficiencySimplePolicy( PolicyBase ):

  def evaluate( self ):
    """
    Evaluate policy on pilots stats, using args (tuple).

    returns:
        {
          'Status': Active|Probing|Bad,
          'Reason':'PilotsEff:low|PilotsEff:med|PilotsEff:good',
        }
    """

    commandResult = super( PilotsEfficiencySimplePolicy, self ).evaluate()
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

    result[ 'Reason' ] = 'Simple pilots Efficiency: '
    if commandResult != 'Idle':
      result[ 'Reason' ] = result[ 'Reason' ] + commandResult
    else:
      result[ 'Reason' ] = 'No values to take a decision'
      
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF