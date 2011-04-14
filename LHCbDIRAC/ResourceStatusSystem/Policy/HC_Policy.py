########################################################################
# $HeadURL:
########################################################################

""" The HC_Policy class is a policy class satisfied when ...
    TO BE DECIDED
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class HC_Policy( PolicyBase ):

  def evaluate( self ):
    """ 
    Evaluate policy on possible ongoing or scheduled downtimes. 
        
    Example of dictionary returned by HC:
    
    {'load_average': None, 
        'c_cf': 0.0, 
        'local_batch_id': None, 
        'total': 10, 
        'clouds': 'ES', 
        'total_cpu_time': None, 
        'completed': 0, 
        'sites': 'LCG.UNIZAR.es', 
        'submitted': 10, 
        'wallclock': None, 
        'f_t': 0.0, 
        'failed': 0, 
        'running': 0, 
        'nr_clouds': 1, 
        'nr_sites': 1, 
        'memory': None, 
        'scaled_cpu_time': None, 
        'intermediumtime': None, 
        'id': 110, 
        'norm_cpu_time': None, 'c_t': 0.0}
            
        
    :returns:
        { 
          'SAT':True|False, 
          'Status':Active|Probing|Bad|Banned, 
          'Reason':'DT:None'|'DT:OUTAGE|'DT:AT_RISK',
          'EndDate':datetime (if needed)
        }
        
    """

    status = super( HC_Policy, self ).evaluate()
 
    #print 'HC_Policy' 

    #print status

    result = { 'SAT':False, 
               'Status':self.args[2], 
               'Reason':'None'}

    if status is None:
      result['SAT'] = None
    
    elif len(status) == 0:
      pass 

    else:
      
      completed = float( status['completed'] )
      failed    = float( status['failed'] )
      submitted = float( status['submitted'] )
      running   = float( status['running'] )
      total     = float( status['total'] )
      
      cf = completed + failed
      
      if not cf:
        if total > 0:
          result['reason'] = 'No job finished'
        else:
          result['reason'] = 'No jobs at all'  
      
      else:  
        efficiency = completed / cf
        
        if efficiency < 0.95:
          result['reason'] = 'Efficiency of %s' % efficiency
      
        elif efficiency >= 0.95:
          result['SAT']    = True
          result['Status'] = 'Active'
          result['Reason'] = 'Efficiency of %s' % efficiency
        
      #elif completed + failed == 0 and total > 0:
      #  result['reason'] = 'No job finished'   
      # 
      #elif total == 0:
      #  result['reason'] = 'No jobs at all'

    #print result 

    return result
#    return {'SAT':False,'Status':'Active','Reason':'Testing'}

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
