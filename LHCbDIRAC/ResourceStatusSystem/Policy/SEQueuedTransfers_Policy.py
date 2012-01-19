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
    Evaluate policy on SE Queued Transfers. Use
    SLS_Command/SLSServiceInfo_Command.  Result of the command is a
    dictionary with SLS infos, or None in case of failure.

    :returns:
        {
          'Status':Error|Active|Bad|Banned,
          'Reason': high, low, mid-high
        }
    """

    status = super(SEQueuedTransfers_Policy, self).evaluate()

    if not status:
      return {'Status': 'Error', "Reason": "SLS_Command ERROR"}

    status = int(status['Queued transfers']) # type float, but represent an int, no need to round.

    if status > 100  : self.result['Status'] = 'Banned'; comment = "high"
    elif status < 70 : self.result['Status'] = 'Active'; comment = "low"
    else             : self.result['Status'] = 'Bad';    comment = "mid-high"

    self.result['Reason'] = "Queued transfers on the SE: %d (%s)" % (status, comment)

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
