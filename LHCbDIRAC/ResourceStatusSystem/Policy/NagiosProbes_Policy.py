# $HeadURL: $
''' NagiosProbes_Policy
  
  The NagiosProbes_Policy checks the nagios probes.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id: $'

class NagiosProbes_Policy( PolicyBase ):
  
  def evaluate(self):
    '''
      We are ignoring the UNKNOWN metricStatuses, as this is a prototype.
      The WARNING metricStatus is ambiguous, so it is ignored until we have
      a clarification.
    '''
  
    _KNOWN_METRIC_STATUS = [ 'OK', 'WARNING', 'CRITICAL', 'UNKNOWN' ]
                    
    probes   = super( NagiosProbes_Policy, self ).evaluate()  
    result = {}
    result[ 'Status' ] = 'Unknown'
    result[ 'Reason' ] = 'No values to take a decision'

    if status is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not probes[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = probes[ 'Message' ]
      return result      
    
    probes = probes[ 'Value' ]
        
    for k in probes.keys():
      if not k in _KNOWN_METRIC_STATUS:
        result[ 'Status' ] = 'Error'
        result[ 'Reason' ] = '%s is not a valid MetricStatus' % k
        return result
    
    if probes.has_key( 'CRITICAL' ):
      result[ 'Status' ] = 'Banned'
      result[ 'Reason' ] = '%d CRITICAL Nagios probes' % probes[ 'CRITICAL' ][ 1 ]
    
    #Only if there is all Ok we return Active
    elif probes.keys() == [ 'OK' ]:
      result[ 'Status' ] = 'Active'
      result[ 'Reason' ] = 'All OK Nagios probes'
    
    return result
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    