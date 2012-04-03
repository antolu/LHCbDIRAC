# $HeadURL$
''' FakePolicy

  Module used for testing purposes.

'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class FakePolicy( PolicyBase ):

  def evaluate( self ):
    """
    """

    commandResult = super( FakePolicy, self ).evaluate()

    return { 'Status' : 'Unknown' }

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF