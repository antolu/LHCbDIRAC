########################################################################
# $HeadURL:
########################################################################

""" The JobsEfficiency_Simple_Policy class is a policy class
    that checks the efficiency of the pilots
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class JobsEfficiency_Simple_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on jobs stats, using args (tuple).

    :returns:
      {
        'Status':Unknown|Active|Probing|Bad,
        'Reason':'JobsEff:Good|JobsEff:Fair|JobsEff:Poor|JobsEff:Bad|JobsEff:Idle',
      }
  """

    status = super(JobsEfficiency_Simple_Policy, self).evaluate()

    if status == 'Unknown':
      return {'Status':'Unknown'}

    self.result['Reason'] = 'Simple Jobs Efficiency: '

    if status == 'Good':
      self.result['Status'] = 'Active'
    elif status == 'Fair':
      self.result['Status'] = 'Active'
    elif status == 'Poor':
      self.result['Status'] = 'Probing'
    elif status == 'Idle':
      self.result['Status'] = 'Unknown'
    elif status == 'Bad':
      self.result['Status'] = 'Bad'

    if status != 'Idle':
      self.result['Reason'] = self.result['Reason'] + status

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
