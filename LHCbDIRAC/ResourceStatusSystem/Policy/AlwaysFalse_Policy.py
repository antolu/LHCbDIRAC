########################################################################
# $HeadURL:
########################################################################

""" The AlwaysFalse_Policy class is a policy class that... checks nothing!
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class AlwaysFalse_Policy(PolicyBase):

  def evaluate(self):
    """
    Does nothing.

    Always returns:
        {
          'Status':'Active'
          'Reason':None
        }
    """

    return {'Status':'Active', 'Reason':'None'}

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
