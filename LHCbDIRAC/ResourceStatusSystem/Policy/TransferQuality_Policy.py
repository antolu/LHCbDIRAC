########################################################################
# $HeadURL:
########################################################################

""" The DataQuality_Policy class is a policy class to check the data quality.
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase
from LHCbDIRAC.ResourceStatusSystem.Policy import Configurations

class TransferQuality_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on Data quality.

    :returns:
        {
          'Status':Error|Unknown|Active|Probing|Banned,
          'Reason':'TransferQuality:None'|'TransferQuality:xx%',
        }
    """

    quality = super(TransferQuality_Policy, self).evaluate()

    if quality == None:
      self.result['Status'] = 'Error'
      return self.result
    elif quality == 'Unknown':
      return {'Status':'Unknown'}

    quality = int( round( quality ) )

    if 'FAILOVER'.lower() in self.args[1].lower():

      self.result['Reason'] = 'TransferQuality: %d %% -> ' % quality
      if quality < Configurations.Transfer_QUALITY_LOW :
        self.result['Status'] = 'Probing'
        strReason = 'Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        self.result['Status'] = 'Active'
        strReason = 'High'
      else:
        self.result['Status'] = 'Active'
        strReason = 'Mean'

      self.result['Reason'] = self.result['Reason'] + strReason

    else:

      self.result['Reason'] = 'TransferQuality: %d %% -> ' % quality
      if quality < Configurations.Transfer_QUALITY_LOW :
        self.result['Status'] = 'Bad'
        strReason = 'Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        self.result['Status'] = 'Active'
        strReason = 'High'
      elif quality >= Configurations.Transfer_QUALITY_LOW and quality < Configurations.Transfer_QUALITY_HIGH:
        self.result['Status'] = 'Probing'
        strReason = 'Mean'

      self.result['Reason'] = self.result['Reason'] + strReason

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
