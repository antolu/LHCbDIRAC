########################################################################
# $HeadURL:
########################################################################

""" The SEQueuedTransfers_Policy class is a policy class satisfied when a SE has a high number of
    queued transfers.
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class SEQueuedTransfers_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on SE Queued Transfers, using args (tuple).

    :returns:
        {
          'Status':Error|Unknown|Active|Probing|Bad,
          'Reason':'QueuedTransfers:High'|'QueuedTransfers:Mid-High'|'QueuedTransfers:Low',
        }
    """

    status = super(SEQueuedTransfers_Policy, self).evaluate()

    if status is None or status == -1:
      return {'Status': 'Error'}

    if status == 'Unknown':
      return {'Status':'Unknown'}

    status = int( round( status['Queued transfers'] ) )

    if status > 100:
      self.result['Status'] = 'Bad'
    elif status < 70:
      self.result['Status'] = 'Active'
    else:
      self.result['Status'] = 'Probing'

    if status is not None and status != -1:

      self.result['Reason'] = "Queued transfers on the SE: %d -> " % status

      if status > 100:
        str_ = 'HIGH'
      elif status < 70:
        str_ = 'Low'
      else:
        str_ = 'Mid-High'

      self.result['Reason'] = self.result['Reason'] + str_

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
