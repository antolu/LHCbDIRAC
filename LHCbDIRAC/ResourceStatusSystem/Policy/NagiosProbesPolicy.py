''' LHCbDIRAC.ResourceStatusSystem.Policy.NagiosProbesPolicy

   NagiosProbesPolicy.__bases__:
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

class NagiosProbesPolicy( PolicyBase ):
  '''
  The NagiosProbesPolicy checks the nagios probes.

  NagiosProbesPolicy, given the Nagios probes in activeMQ for this element,
  proposes a new status.
  '''

  def evaluate(self):
    '''
      We are ignoring the UNKNOWN metricStatuses, as this is a prototype.
      The WARNING metricStatus is ambiguous, so it is ignored until we have
      a clarification.
    '''

    _KNOWN_METRIC_STATUS = [ 'OK', 'WARNING', 'CRITICAL', 'UNKNOWN' ]

    commandResult   = super( NagiosProbesPolicy, self ).evaluate()
    result = {}
    result[ 'Status' ] = 'Unknown'
    result[ 'Reason' ] = 'No values to take a decision'

    if commandResult is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return result

    commandResult = commandResult[ 'Value' ]

    for k in commandResult.keys():
      if not k in _KNOWN_METRIC_STATUS:
        result[ 'Status' ] = 'Error'
        result[ 'Reason' ] = '%s is not a valid MetricStatus' % k
        return result

    if commandResult.has_key( 'CRITICAL' ):
      result[ 'Status' ] = 'Banned'
      result[ 'Reason' ] = '%d CRITICAL Nagios probes' % commandResult[ 'CRITICAL' ][ 1 ]

    #Only if there is all Ok we return Active
    elif commandResult.keys() == [ 'OK' ]:
      result[ 'Status' ] = 'Active'
      result[ 'Reason' ] = 'All OK Nagios probes'

    return result

#...............................................................................
#EOF
