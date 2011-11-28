################################################################################
# $HeadURL:
################################################################################

""" The NagiosProbes_Policy checks the nagios probes
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class NagiosProbes_Policy(PolicyBase):
  
  def evaluate(self):
    '''
      We are ignoring the UNKNOWN metricStatuses, as this is a prototype.
      The WARNING metricStatus is ambiguous, so it is ignored until we have
      a clarification.
    '''
  
    _KNOWN_METRIC_STATUS = [ 'OK', 'WARNING', 'CRITICAL', 'UNKNOWN' ]
  
    self.result[ 'Status' ] = 'Unknown'
    self.result[ 'Reason' ] = 'Unknown'
                    
    results   = super( NagiosProbes_Policy, self ).evaluate()    
        
    for k in results.keys():
      if not k in _KNOWN_METRIC_STATUS:
        self.result[ 'Status' ] = 'Error'
        self.result[ 'Reason' ] = '%s is not a valid MetricStatus' % k
        
        return self.result
    
    if results.has_key( 'CRITICAL' ):
      self.result[ 'Status' ] = 'Banned'
      self.result[ 'Reason' ] = '%d CRITICAL Nagios probes' % results[ 'CRITICAL' ][ 1 ]
    
    #Only if there is all Ok we return Active
    elif results.keys() == ['OK']:
      self.result[ 'Status' ] = 'Active'
      self.result[ 'Reason' ] = 'All OK Nagios probes'
    
    return self.result
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF       
     
    