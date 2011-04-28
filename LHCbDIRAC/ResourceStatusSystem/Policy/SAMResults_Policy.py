########################################################################
# $HeadURL:
########################################################################

""" The SAMself.results_Policy class is a policy class that checks
    the SAM job self.results
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SAMResults_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on SAM jobs self.results.

    :return:
        {
          'Status':Error|Unknown|Active|Probing|Banned,
          'Reason':'SAMRes:ok|down|na|degraded|partial|maint',
        }
    """

    SAMstatus = super(SAMResults_Policy, self).evaluate()

    if SAMstatus is None:
      return {'Status':'Error'}

    if SAMstatus == 'Unknown':
      return {'Status':'Unknown'}

    status = 'ok'

    for s in SAMstatus.values():
      if s == 'error':
        status = 'down'
        break
      elif s == 'down':
        status = 'down'
        break
      elif s == 'warn':
        status = 'degraded'
        break
      elif s == 'maint':
        status = 'maint'
        break

    if status == 'ok':
      na = True
      for s in SAMstatus.values():
        if s != 'na':
          na = False
          break
      if na == True:
        status = 'na'

    self.result['Reason'] = 'SAM status: '

    if status == 'ok':
      self.result['Status'] = 'Active'
    elif status == 'down':
      self.result['Status'] = 'Bad'
    elif status == 'na':
      self.result['Status'] = 'Unknown'
    elif status == 'degraded':
      self.result['Status'] = 'Probing'
    elif status == 'maint':
      self.result['Status'] = 'Bad'

    if status != 'na':
      self.result['Reason'] = self.result['Reason'] + status

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
