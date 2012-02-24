# $HeadURL$
''' Fake_Confirm_Policy

  Module used for testing purposes.

'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class Fake_Confirm_Policy( PolicyBase ):

  def evaluate(self):
    """
    """

    Fake_R = super(Fake_Confirm_Policy, self).evaluate()

    return { 'Status' : 'Active', 'Reason' : 'fake' }

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
