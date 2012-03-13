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

    status = super( PilotsEfficiencySimplePolicy, self ).evaluate()
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

    if status == 'Good':
      result[ 'Status' ] = 'Active'
    elif status == 'Fair':
      result[ 'Status' ] = 'Active'
    elif status == 'Poor':
      result[ 'Status' ] = 'Probing'
    elif status == 'Idle':
      result[ 'Status' ] = 'Unknown'
    elif status == 'Bad':
      result[ 'Status' ] = 'Bad'

    result[ 'Reason' ] = 'Simple pilots Efficiency: '
    if status != 'Idle':
      result[ 'Reason' ] = result[ 'Reason' ] + status
    else:
      result[ 'Reason' ] = 'No values to take a decision'
      
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF