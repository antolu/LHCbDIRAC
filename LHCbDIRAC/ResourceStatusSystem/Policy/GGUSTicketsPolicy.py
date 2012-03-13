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

    GGUS_N = super( GGUSTicketsPolicy, self ).evaluate()
    result = {}

    if GGUS_N is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not GGUS_N[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = GGUS_N[ 'Message' ]
      return result

    GGUS_N = GGUS_N[ 'Value' ]

    if GGUS_N is None:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return result
    elif GGUS_N == 0:
      result[ 'Status' ] = 'Active'
      result[ 'Reason' ] = 'NO GGUSTickets unsolved'
    else:
      result[ 'Status' ] = 'Probing'
      result[ 'Reason' ] = 'GGUSTickets unsolved: %d' % ( GGUS_N )     

    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF