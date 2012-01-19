########################################################################
# $HeadURL:
########################################################################

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class Fake_Confirm_Policy(PolicyBase):

  def evaluate(self):
    """
    """

    Fake_R = super(Fake_Confirm_Policy, self).evaluate()

    return {'Status': 'Active', 'Reason':'fake'}

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
