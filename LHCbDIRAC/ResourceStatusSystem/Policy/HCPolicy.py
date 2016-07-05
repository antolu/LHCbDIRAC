#''' HCPolicy
#
#  The HCPolicy class is a policy class satisfied when ...
#  TO BE DECIDED.
#
#'''
#
#from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase
#
#__RCSID__ = "$Id$"

#...............................................................................
#
#
# OBSOLETE CODE. TODO: refactor it !
#
#...............................................................................

#class HCPolicy( PolicyBase ):
#
#  def evaluate( self ):
#    """
#    Evaluate policy on possible ongoing or scheduled downtimes.
#
#    Example of dictionary returned by HC:
#
#    {'load_average': None,
#        'c_cf': 0.0,
#        'local_batch_id': None,
#        'total': 10,
#        'clouds': 'ES',
#        'total_cpu_time': None,
#        'completed': 0,
#        'sites': 'LCG.UNIZAR.es',
#        'submitted': 10,
#        'wallclock': None,
#        'f_t': 0.0,
#        'failed': 0,
#        'running': 0,
#        'nr_clouds': 1,
#        'nr_sites': 1,
#        'memory': None,
#        'scaled_cpu_time': None,
#        'intermediumtime': None,
#        'id': 110,
#        'norm_cpu_time': None, 'c_t': 0.0}
#
#
#    :returns:
#        {
#          'SAT':True|False,
#          'Status':Active|Probing|Bad|Banned,
#          'Reason':'DT:None'|'DT:OUTAGE|'DT:AT_RISK',
#          'EndDate':datetime (if needed)
#        }
#
#    """
#
#    commandResult = super( HCPolicy, self ).evaluate()
#
#    result = { 'SAT':False,
#               'Status':self.args[2],
#               'Reason':'None'}
#
#    if commandResult is None:
#      result['SAT']    = None
#      result['reason'] = 'No scheduled,ongoing or finished HC test'
#
#    elif len( commandResult ) == 0:
#      result['reason'] = 'HC has no opinion'
#      pass
#
#    else:
#
#      completed = float( commandResult['completed'] )
#      failed    = float( commandResult['failed'] )
#      submitted = float( commandResult['submitted'] )
#      running   = float( commandResult['running'] )
#      total     = float( commandResult['total'] )
#
#      cf = completed + failed
#
#      if not cf:
#        if total > 0:
#          result['reason'] = 'No job finished'
#        else:
#          result['reason'] = 'No jobs at all'
#
#      else:
#        efficiency = completed / cf
#
#        if efficiency < 0.95:
#          result['reason'] = 'Efficiency of %s' % efficiency
#
#        elif efficiency >= 0.95:
#          result['SAT']    = True
#          result['Status'] = 'Active'
#          result['Reason'] = 'Efficiency of %s' % efficiency
#
#    return result
#
#  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
