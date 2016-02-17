''' LHCbDIRAC.ResourceStatusSystem.Policy.SAMResultsPolicy

   SAMResultsPolicy.__bases__:
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

class SAMResultsPolicy( PolicyBase ):
  '''
  The SAMResultsPolicy class is a policy class that checks
  the SAM job self.results.

  SAMResultsPolicy, given the SAM status for the element, proposes a new
  status.
  '''

  def evaluate(self):
    """
    Evaluate policy on SAM jobs self.results.

    :return:
        {
          'Status':Error|Unknown|Active|Probing|Banned,
          'Reason':'SAMRes:ok|down|na|degraded|partial|maint',
        }
    """

    commandResult = super( SAMResultsPolicy, self ).evaluate()
    result = {}

    if commandResult is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return result

    commandResult = commandResult[ 'Value' ]
    
    result[ 'Status' ] = 'Active'
    status             = 'ok'

    for samStatus in commandResult.values():
      if samStatus == 'error':
        status = 'down'
        result[ 'Status' ] = 'Bad'
        break
      elif samStatus == 'down':
        status = 'down'
        result[ 'Status' ] = 'Bad'
        break
      elif samStatus == 'warn':
        status = 'degraded'
        result[ 'Status' ] = 'Probing'
        break
      elif samStatus == 'maint':
        status = 'maint'
        result[ 'Status' ] = 'Bad'
        break

    if status == 'ok':
      na = True
      for samStatus in commandResult.values():
        if samStatus != 'na':
          na = False
          break

      if na == True:
        status = 'na'
        result[ 'Status' ] = 'Unknown'

    result[ 'Reason' ] = 'SAM status: '
    if status != 'na':
      result[ 'Reason' ] = result[ 'Reason' ] + status

    return result

#...............................................................................
#EOF
