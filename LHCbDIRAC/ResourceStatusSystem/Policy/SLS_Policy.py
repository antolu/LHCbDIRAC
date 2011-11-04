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
        'Status':Error|Unknown|Active|Banned,
        'Reason':'Availability:High'|'Availability:Mid-High'|'Availability:Low',
      }
    """

    # Execute the command and returns a value as a string.
    status = super(SLS_Policy, self).evaluate()

    if status == 'Unknown':
      return {'Status':'Unknown'}

    if status == None or status == -1:
      return {'Status': 'Error'}

    # Here, status is not "Unknown", None, or -1

    # FIXME: Put thresholds in the CS.
    if status < 40:
      self.result['Status'] = 'Bad'
    elif status > 90:
      self.result['Status'] = 'Active'

    if status > 90:
      str_ = 'High'
    elif status <= 40:
      str_ = 'Poor'
    else:
      str_ = 'Sufficient'

    self.result['Reason'] = "SLS availability: %d %% -> %s" % (status, str_)

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
