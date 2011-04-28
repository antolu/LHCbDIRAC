########################################################################
# $HeadURL:
########################################################################

""" The SEOccupancy_Policy class is a policy class satisfied when a SE has a high occupancy
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SEOccupancy_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on SE occupancy.

   :returns:
      {
        'Status':Error|Unknown|Active|Probing|Banned,
        'Reason':'SE_Occupancy:High'|'SE_Occupancy:Mid-High'|'SE_Occupancy:Low',
      }
    """

    status = super(SEOccupancy_Policy, self).evaluate()

    if status == 'Unknown':
      return {'Status':'Unknown'}

    if status is None or status == -1:
      self.result['Status'] = 'Error'

    else:
      if status == 0:
        self.result['Status'] = 'Banned'
      elif status > 10:
        self.result['Status'] = 'Active'
      else:
        self.result['Status'] = 'Probing'

    if status is not None and status != -1:
      self.result['Reason'] = "Space availability: %d %% -> " % ( status )

      if status == 0:
        str_ = 'NONE!'
      else:
        if status > 30:
          str_ = 'High'
        elif status <= 10:
          str_ = 'Poor'
        else:
          str_ = 'Sufficient'

      self.result['Reason'] = self.result['Reason'] + str_


    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
