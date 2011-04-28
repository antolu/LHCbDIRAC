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

    if GGUS_N == 'Unknown':
      return {'Status':'Unknown'}

    if GGUS_N >= 1:
      self.result['Status'] = 'Probing'
      self.result['Reason'] = 'GGUSTickets unsolved: %d' % ( GGUS_N )
    else:
      self.result['Status'] = 'Active'
      self.result['Reason'] = 'NO GGUSTickets unsolved'

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
