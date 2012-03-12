# $HeadURL$
''' Fake_Policy

  Module used for testing purposes.

'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class Fake_Policy( PolicyBase ):

  def evaluate(self):
    """
    """

    Fake_R = super(Fake_Policy, self).evaluate()

    return { 'Status' : 'Unknown' }

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF