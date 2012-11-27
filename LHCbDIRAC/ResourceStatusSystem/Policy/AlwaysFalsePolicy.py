''' AlwaysFalsePolicy

  Class is a policy class that... checks nothing!
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class AlwaysFalsePolicy( PolicyBase ):
  '''
  AlwaysFalsePolicy is a prototype that can be used for insipiration. Note that
  it is not invoking any Command.
  '''

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
  
################################################################################  
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF