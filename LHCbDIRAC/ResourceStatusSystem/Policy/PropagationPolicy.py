''' LHCbDIRAC.ResourceStatusSystem.Policy.PropagationPolicy

   PropagationPolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase

'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = "$Id$"

#...............................................................................
#
#
# OBSOLETE CODE. TODO: refactor it !
#
#...............................................................................

#class PropagationPolicy( PolicyBase ):
#  '''
#  The PropagationPolicy module is a policy module used to update the status of
#  a validRes, based on statistics of its services (for the site),
#  of its nodes (for the services), or of its SE (for the Storage services).
#
#  PropagationPolicy, given the status(es) of the element children, proposes a new
#  status.
#  '''
#
#  def evaluate( self ):
#    """
#    Propagation policy on Site or Service, using args (tuple).
#    It will get Services or nodes or SE stats.
#
#    :returns:
#      {
#      `Status`:Error|Unknown|Active|Banned,
#      `Reason`:'A:X/P:Y/B:Z'
#      }
#    """
#
#    commandResult = super( PropagationPolicy, self ).evaluate()
#    result = {}
#
#    if commandResult is None:
#      result[ 'Status' ] = 'Error'
#      result[ 'Reason' ] = 'Command evaluation returned None'
#      return result
#
#    if not commandResult[ 'OK' ]:
#      result[ 'Status' ] = 'Error'
#      result[ 'Reason' ] = commandResult[ 'Message' ]
#      return result
#
#    commandResult = commandResult[ 'Value' ]
#
#    if ( commandResult[ 'Active' ] > 0 and commandResult[ 'Probing' ] == 0 and
#         commandResult[ 'Bad' ] == 0 and commandResult[ 'Banned' ] == 0 ):
#      status = 'Active'
#    elif ( commandResult[ 'Active' ] == 0 and commandResult[ 'Probing' ] == 0 and
#           commandResult[ 'Bad' ] == 0 and commandResult[ 'Banned' ] > 0 ):
#      status = 'Banned'
#    elif ( commandResult[ 'Active' ] > 0 or commandResult[ 'Probing' ] > 0 or
#           commandResult[ 'Bad' ] > 0 or commandResult[ 'Banned' ] > 0 ):
#      status = 'Bad'
#    else:
#      status = 'Unknown'
#
#    result[ 'Status' ] = status
#    # TODO: Check that self.args[2] is correct, in the future, use
#    # named fields instead of numbers
#    _msg = '%s: Active:%d, Probing :%d, Bad: %d, Banned:%d'
#    result[ 'Reason' ] = _msg % ( self.args[ 2 ], commandResult[ 'Active' ],
#                                  commandResult[ 'Probing' ], commandResult[ 'Bad' ],
#                                  commandResult[ 'Banned' ] )
#
#    return result

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
