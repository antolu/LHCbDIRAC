# $HeadURL$
''' SAMResults_Policy

  The SAMResults_Policy class is a policy class that checks
  the SAM job self.results.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class SAMResults_Policy( PolicyBase ):

  def evaluate(self):
    """
    Evaluate policy on SAM jobs self.results.

    :return:
        {
          'Status':Error|Unknown|Active|Probing|Banned,
          'Reason':'SAMRes:ok|down|na|degraded|partial|maint',
        }
    """

    SAMstatus = super( SAMResults_Policy, self ).evaluate()
    result = {}

    if SAMstatus is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not SAMstatus[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = SAMstatus[ 'Message' ]
      return result

    SAMstatus = SAMstatus[ 'Value' ]
    
    result[ 'Status' ] = 'Active'
    status             = 'ok'

    for s in SAMstatus.values():
      if s == 'error':
        status = 'down'
        result[ 'Status' ] = 'Bad'
        break
      elif s == 'down':
        status = 'down'
        result[ 'Status' ] = 'Bad'
        break
      elif s == 'warn':
        status = 'degraded'
        result[ 'Status' ] = 'Probing'
        break
      elif s == 'maint':
        status = 'maint'
        result[ 'Status' ] = 'Bad'
        break

    if status == 'ok':
      na = True
      for s in SAMstatus.values():
        if s != 'na':
          na = False
          break

      if na == True:
        status = 'na'
        result[ 'Status' ] = 'Unknown'

    result[ 'Reason' ] = 'SAM status: '
    if status != 'na':
      result[ 'Reason' ] = result[ 'Reason' ] + status

    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF