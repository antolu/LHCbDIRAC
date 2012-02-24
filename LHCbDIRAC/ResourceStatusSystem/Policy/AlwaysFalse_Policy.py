# $HeadURL$
''' AlwaysFalse_Policy

  Class is a policy class that... checks nothing!
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class AlwaysFalse_Policy( PolicyBase ):

  def evaluate(self):
    """
    Does nothing.

    Always returns:
        {
          'Status':'Active'
          'Reason':None
        }
    """

    return { 'Status' : 'Active', 'Reason' : 'This is the AlwasyFalse policy' }

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
