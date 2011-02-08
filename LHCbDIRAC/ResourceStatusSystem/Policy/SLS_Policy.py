########################################################################
# $HeadURL:
########################################################################

""" The SLS_Policy class is a policy class satisfied when a SLS sensors report problems
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SLS_Policy( PolicyBase ):

  def evaluate( self ):
    """ 
    Evaluate policy on SLS availability. 

   :returns:
      { 
        'SAT':True|False, 
        'Status':Active|Probing|Banned, 
        'Reason':'Availability:High'|'Availability:Mid-High'|'Availability:Low',
      }
    """

    status = super( SLS_Policy, self ).evaluate()

    if status == 'Unknown':
      return {'SAT':'Unknown'}

    if status is None or status == -1:
      self.result['SAT'] = None
    else:
      if self.oldStatus == 'Active':
        if status < 40:
          self.result['SAT'] = True
          self.result['Status'] = 'Bad'
        elif status > 90:
          self.result['SAT'] = False
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'

      elif self.oldStatus == 'Probing':
        if status < 40:
          self.result['SAT'] = True
          self.result['Status'] = 'Bad'
        elif status > 90:
          self.result['SAT'] = True
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = False
          self.result['Status'] = 'Probing'

      elif self.oldStatus == 'Bad':
        if status < 40:
          self.result['SAT'] = True
          self.result['Status'] = 'Banned'
        elif status > 90:
          self.result['SAT'] = True
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'

      elif self.oldStatus == 'Banned':
        if status < 40:
          self.result['SAT'] = False
          self.result['Status'] = 'Banned'
        elif status > 90:
          self.result['SAT'] = True
          self.result['Status'] = 'Active'
        else:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'


    if status is not None and status != -1:
      self.result['Reason'] = "SLS availability: %d %% -> " % ( status )

      if status > 90:
        str = 'High'
      elif status <= 40:
        str = 'Poor'
      else:
        str = 'Sufficient'

      self.result['Reason'] = self.result['Reason'] + str


    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
