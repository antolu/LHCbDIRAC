''' FakePolicy

  Module used for testing purposes.

'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class FakePolicy( PolicyBase ):
  '''
  FakePolicy, is a prototype that can be used for insipiration. Note that
  it IS invoking a command ( if specified on Configurations ).
  '''

  def evaluate( self ):
    '''
    Command evaluation.
    '''

    __commandResult__ = super( FakePolicy, self ).evaluate()

    return { 'Status' : 'Unknown' }

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF