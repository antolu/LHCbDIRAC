########################################################################
# $HeadURL:
########################################################################

""" The GGUSTickets_Policy class is a policy class that evaluates on
    how many tickets are open atm.
"""

__RCSID__ = "$Id"

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class GGUSTickets_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on opened tickets, using args (tuple).

    :returns:
        {
          'Status':Active|Probing,
          'Reason':'GGUSTickets: n unsolved',
        }
    """

    GGUS_N = super(GGUSTickets_Policy, self).evaluate()
    result = {}

    if not GGUS_N[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = GGUS_N[ 'Message' ]
      return result

    GGUS_N = GGUS_N[ 'Value' ]

#    elif GGUS_N == 'Unknown':
#      return { 'Status' : 'Unknown' }

    if GGUS_N == 0:
      result[ 'Status' ] = 'Active'
      result[ 'Reason' ] = 'NO GGUSTickets unsolved'
    else:
      result[ 'Status' ] = 'Probing'
      result[ 'Reason' ] = 'GGUSTickets unsolved: %d' % ( GGUS_N )     

    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
