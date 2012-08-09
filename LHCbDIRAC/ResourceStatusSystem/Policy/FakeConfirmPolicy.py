# $HeadURL$
''' FakeConfirmPolicy

  Module used for testing purposes.

'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class FakeConfirmPolicy( PolicyBase ):

  def evaluate( self ):
    """
    """

    commandResult = super( FakeConfirmPolicy, self ).evaluate()

    return { 'Status' : 'Active', 'Reason' : 'fake' }

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################ 
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF