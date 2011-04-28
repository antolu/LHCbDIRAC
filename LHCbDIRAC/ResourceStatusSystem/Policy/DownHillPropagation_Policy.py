########################################################################
# $HeadURL:
########################################################################

""" The DownHillPropagation_Policy module is a policy module used to update the status of
    an element, based on how its element in the upper part of the hierarchy is behaving in the RSS
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class DownHillPropagation_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on "upper" element Status, using args (tuple).
    The status is propagated only when one of the two status is 'Banned'

    :returns:
        {
          `Status`:Error|Unknown|Active|Probing|Banned,
          `Reason`:'Node/Site status: Active|Probing|Banned'
        }
    """

    resourceStatus = super(DownHillPropagation_Policy, self).evaluate()

    if resourceStatus is None:
      return {'Status':'Error'}

    elif resourceStatus == 'Unknown':
      return {'Status':'Unknown'}

    else:
      self.result['Status'] = resourceStatus
      self.result['Reason'] = 'Site/Node status: ' + resourceStatus
      return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
