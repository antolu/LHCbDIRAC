########################################################################
# $HeadURL:
########################################################################

""" The SLS_Policy class is a policy class satisfied when a SLS sensors report problems
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SLS_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on SLS availability.

   :returns:
      {
        'Status':Error|Unknown|Active|Probing|Banned,
        'Reason':'Availability:High'|'Availability:Mid-High'|'Availability:Low',
      }
    """

    status = super(SLS_Policy, self).evaluate()

    if status == 'Unknown':
      return {'Status':'Unknown'}

    if status is None or status == -1:
      self.result['Status'] = 'Error'

    else:
      if status < 40:
        self.result['Status'] = 'Bad'
      elif status > 90:
        self.result['Status'] = 'Active'
      else:
        self.result['Status'] = 'Probing'

    if status is not None and status != -1:
      self.result['Reason'] = "SLS availability: %d %% -> " % ( status )

      if status > 90:
        str_ = 'High'
      elif status <= 40:
        str_ = 'Poor'
      else:
        str_ = 'Sufficient'

      self.result['Reason'] = self.result['Reason'] + str_


    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
