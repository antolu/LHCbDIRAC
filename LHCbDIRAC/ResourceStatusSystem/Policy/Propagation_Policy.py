########################################################################
# $HeadURL:
########################################################################

""" The Propagation_Policy module is a policy module used to update the status of
    a validRes, based on statistics of its services (for the site),
    of its nodes (for the services), or of its SE (for the Storage services).
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class Propagation_Policy(PolicyBase):

  def evaluate(self):
    """
    Propagation policy on Site or Service, using args (tuple).
    It will get Services or nodes or SE stats.

    :returns:
      {
      `Status`:Error|Unknown|Active|Banned,
      `Reason`:'A:X/P:Y/B:Z'
      }
    """

    stats = super(Propagation_Policy, self).evaluate()

    if stats is None:
      return {'Status':'Error'}

    if stats == 'Unknown':
      return {'Status':'Unknown', 'Reason':'No info available from service'}

    if stats['Active'] > 0 and stats['Probing'] == 0 and stats['Bad'] == 0 and stats['Banned'] == 0:
      status = 'Active'
    elif stats['Active'] == 0 and stats['Probing'] == 0 and stats['Bad'] == 0 and stats['Banned'] > 0:
      status = 'Banned'
    elif stats['Active'] > 0 or stats['Probing'] > 0 or stats['Bad'] > 0 or stats['Banned'] > 0:
      status = 'Bad'
    else:
      status = 'Unknown'

    self.result['Status'] = status
    # TODO: Check that self.args[2] is correct, in the future, use
    # named fields instead of numbers
    self.result['Reason'] = '%s: Active:%d, Probing :%d, Bad: %d, Banned:%d' % ( self.args[2],
                                                                           stats['Active'],
                                                                           stats['Probing'],
                                                                           stats['Bad'],
                                                                           stats['Banned'] )

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
