# $HeadURL$
''' SAMResultsPolicy

  The SAMResultsPolicy class is a policy class that checks
  the SAM job self.results.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class SAMResultsPolicy( PolicyBase ):

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

    for s in commandResult.values():
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
      for s in commandResult.values():
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