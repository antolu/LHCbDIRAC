# $HeadURL$
''' GGUSTicketsPolicy 
  
  The GGUSTicketsPolicy class is a policy class that evaluates on
  how many tickets are open atm.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class GGUSTicketsPolicy( PolicyBase ):

  def evaluate( self ):
    """
    Evaluate policy on opened tickets, using args (tuple).

    :returns:
        {
          'Status':Active|Probing,
          'Reason':'GGUSTickets: n unsolved',
        }
    """

    commandResult = super( GGUSTicketsPolicy, self ).evaluate()
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

    if commandResult is None:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return result
    elif commandResult == 0:
      result[ 'Status' ] = 'Active'
      result[ 'Reason' ] = 'NO GGUSTickets unsolved'
    else:
      result[ 'Status' ] = 'Probing'
      result[ 'Reason' ] = 'GGUSTickets unsolved: %d' % ( commandResult )     

    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF