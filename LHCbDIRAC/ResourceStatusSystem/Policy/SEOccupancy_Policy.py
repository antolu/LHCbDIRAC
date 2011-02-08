########################################################################
# $HeadURL:
########################################################################

""" The SEOccupancy_Policy class is a policy class satisfied when a SE has a high occupancy
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SEOccupancy_Policy( PolicyBase ):

  def evaluate( self ):
    """ 
    Evaluate policy on SE occupancy. 

   :returns:
      { 
        'SAT':True|False, 
        'Status':Active|Probing|Banned, 
        'Reason':'SE_Occupancy:High'|'SE_Occupancy:Mid-High'|'SE_Occupancy:Low',
      }
    """

    status = super( SEOccupancy_Policy, self ).evaluate()

    if status == 'Unknown':
      return {'SAT':'Unknown'}

    if status is None or status == -1:
      self.result['SAT'] = None
    else:
      if self.oldStatus == 'Active':
        if status == 0:
          self.result['SAT'] = True
          self.result['Status'] = 'Banned'
        elif status > 10:
          self.result['SAT'] = False
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'

      elif self.oldStatus == 'Probing':
        if status == 0:
          self.result['SAT'] = True
          self.result['Status'] = 'Banned'
        elif status > 10:
          self.result['SAT'] = True
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = False
          self.result['Status'] = 'Probing'

      elif self.oldStatus == 'Bad':
        if status == 0:
          self.result['SAT'] = True
          self.result['Status'] = 'Banned'
        elif status > 10:
          self.result['SAT'] = True
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'

      elif self.oldStatus == 'Banned':
        if status == 0:
          self.result['SAT'] = False
          self.result['Status'] = 'Banned'
        elif status > 10:
          self.result['SAT'] = True
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'


    if status is not None and status != -1:
      self.result['Reason'] = "Space availability: %d %% -> " % ( status )

      if status == 0:
        str = 'NONE!'
      else:
        if status > 30:
          str = 'High'
        elif status <= 10:
          str = 'Poor'
        else:
          str = 'Sufficient'

      self.result['Reason'] = self.result['Reason'] + str


    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
