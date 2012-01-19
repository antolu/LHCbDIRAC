########################################################################
# $HeadURL:
########################################################################

""" The PilotsEfficiency_Simple_Policy class is a policy class
    that checks the efficiency of the pilots
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class PilotsEfficiency_Simple_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on pilots stats, using args (tuple).

    returns:
        {
          'Status': Active|Probing|Bad,
          'Reason':'PilotsEff:low|PilotsEff:med|PilotsEff:good',
        }
    """

    status = super(PilotsEfficiency_Simple_Policy, self).evaluate()

    if status == 'Unknown':
      return {'Status':'Unknown'}

    if status == None:
      return {'Status':'Error'}

    self.result['Reason'] = 'Simple pilots Efficiency: '

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
